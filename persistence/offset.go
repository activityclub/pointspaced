package persistence

import "strconv"
import "github.com/activityclub/pointspaced/psdcontext"
import "github.com/garyburd/redigo/redis"
import "fmt"
import "sort"
import "strings"
import "errors"

func (self RedisHZ) MultiQueryBucketsWithOffsets(uids, offsets, things []string, atid string, start_ts int64, end_ts int64) (*MUMTResponse, error) {

	if len(uids) != len(offsets) {
		return nil, errors.New("len(uids) != len(offsets)")
	}

	uniq_offsets := make(map[string]map[string]RedisHZRequest)
	sorted_keys := make(map[string][]string)
	for _, o := range offsets {
		oint, _ := strconv.ParseInt(o, 10, 64)
		uniq_offsets[o] = self.requestsForRange(start_ts+oint, end_ts+oint)
		for idx, _ := range uniq_offsets[o] {
			sorted_keys[o] = append(sorted_keys[o], idx)
		}
		sort.Strings(sorted_keys[o])
	}

	r := psdcontext.Ctx.RedisPool.Get()
	defer r.Close()

	minSent := map[string]map[string][]int64{}
	maxSent := map[string]map[string][]int64{}

	for ii, uid := range uids {

		minSent[uid] = map[string][]int64{}
		maxSent[uid] = map[string][]int64{}

		for _, thing := range things {

			minSent[uid][thing] = []int64{}
			maxSent[uid][thing] = []int64{}

			matchThing := thing2id(thing)

			o := offsets[ii]
			for _, rk := range sorted_keys[o] {
				request := uniq_offsets[o][rk]
				key := fmt.Sprintf("hz:%s:%s:%s", matchThing, uid, request.TimeBucket)
				r.Send("HGETALL", key)
				qmin, _ := strconv.ParseInt(request.QueryMin(), 10, 64)
				minSent[uid][thing] = append(minSent[uid][thing], qmin)
				qmax, _ := strconv.ParseInt(request.QueryMax(), 10, 64)
				maxSent[uid][thing] = append(maxSent[uid][thing], qmax)
			}
		}
	}

	r.Flush()

	mumtresp := MUMTResponse{}
	mumtresp.Data = make(map[string]map[string]interface{})

	for _, uid := range uids {
		if mumtresp.Data[uid] == nil {
			mumtresp.Data[uid] = make(map[string]interface{})
		}

		atids := []int64{}
		aids := map[int64]bool{}

		for _, thing := range things {
			sum := int64(0)
			for qidx, qmin := range minSent[uid][thing] {
				qmax := maxSent[uid][thing][qidx]
				reply, _ := redis.MultiBulk(r.Receive())
				lastAtid := ""
				lastAid := ""
				lastBucket := int64(0)
				for i, x := range reply {
					if i%2 == 0 {
						bytes := x.([]byte)
						str := string(bytes)
						tokens := strings.Split(str, ":")
						lastBucket, _ = strconv.ParseInt(tokens[0], 10, 64)
						if len(tokens[1]) > 0 {
							lastAtid = tokens[1]
						}
						if len(tokens[2]) > 0 {
							lastAid = tokens[2]
						}
					} else {
						bytes := x.([]byte)
						str := string(bytes)
						val, err := strconv.ParseInt(str, 10, 64)
						if err == nil {
							if lastBucket <= qmax && lastBucket >= qmin {
								if atid == "all" || lastAtid == atid {
									sum += val
									if lastAid != "" {
										lastAidInt, err := strconv.ParseInt(lastAid, 10, 64)
										if err == nil {
											_, exists := aids[lastAidInt]
											if !exists {
												if lastAtid != "" {
													aids[lastAidInt] = true
													lastAtidInt, err := strconv.ParseInt(lastAtid, 10, 64)
													if err == nil {
														atids = append(atids, lastAtidInt)
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}

			}
			mumtresp.Data[uid][thing] = sum
		}
		mumtresp.Data[uid]["_atids"] = atids
	}

	for idx, m := range mumtresp.Data {
		if len(m["_atids"].([]int64)) == 0 {
			delete(mumtresp.Data, idx)
		} else if m["points"] != nil && m["points"].(int64) == 0 {
			delete(mumtresp.Data, idx)
		}
	}

	return &mumtresp, nil
}
