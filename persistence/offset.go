package persistence

//import "time"
import "strconv"

//import "errors"
import "pointspaced/psdcontext"
import "github.com/garyburd/redigo/redis"

//import "github.com/ugorji/go/codec"
import "fmt"
import "sort"
import "strings"

func (self RedisHZ) MultiQueryBucketsWithOffsets(uids, offsets, things []string, atid string, start_ts int64, end_ts int64) (*MUMTResponse, error) {
	requests := self.requestsForRange(start_ts, end_ts)

	r := psdcontext.Ctx.RedisPool.Get()
	defer r.Close()

	minSent := map[string]map[string][]int64{}
	maxSent := map[string]map[string][]int64{}

	var reqkeys []string
	for idx, _ := range requests {
		reqkeys = append(reqkeys, idx)
	}
	sort.Strings(reqkeys)

	for _, uid := range uids {

		minSent[uid] = map[string][]int64{}
		maxSent[uid] = map[string][]int64{}

		for _, thing := range things {

			minSent[uid][thing] = []int64{}
			maxSent[uid][thing] = []int64{}

			matchThing := thing2id(thing)

			for _, rk := range reqkeys {
				request := requests[rk]
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
