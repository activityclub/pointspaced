package persistence

import "time"

type RedisACRCursor struct {
	Cursor time.Time
	To     time.Time
}

func NewRedisACRCursor(from time.Time, to time.Time) RedisACRCursor {
	c := RedisACRCursor{}
	c.Cursor = from
	c.To = to
	return c

}

func (self *RedisACRCursor) HasReachedEnd() bool {
	if self.Cursor.Unix() > self.To.Unix() {
		return true
	}
	return false
}

func (self *RedisACRCursor) CanJumpYear() bool {
	_, M, D := self.Cursor.Date()
	Hour := self.Cursor.Hour()
	Min := self.Cursor.Minute()
	Sec := self.Cursor.Second()
	if (M == 1) && (D == 1) && (Hour == 0) && (Min == 0) && (Sec == 0) {
		proposed_jump_time := self.Cursor.AddDate(1, 0, 0)
		if proposed_jump_time.Unix() > self.To.Unix() {
			return false
		}
		return true
	}
	return false
}

func (self *RedisACRCursor) CanJumpMonth() bool {
	D := self.Cursor.Day()
	Hour := self.Cursor.Hour()
	Min := self.Cursor.Minute()
	Sec := self.Cursor.Second()
	if (D == 1) && (Hour == 0) && (Min == 0) && (Sec == 0) {
		proposed_jump_time := self.Cursor.AddDate(0, 1, 0)
		if proposed_jump_time.Unix() > self.To.Unix() {
			return false
		}
		return true
	}
	return false
}

func (self *RedisACRCursor) CanJumpDay() bool {
	Hour := self.Cursor.Hour()
	Min := self.Cursor.Minute()
	Sec := self.Cursor.Second()
	if (Hour == 0) && (Min == 0) && (Sec == 0) {
		proposed_jump_time := self.Cursor.AddDate(0, 0, 1)
		if proposed_jump_time.Unix() > self.To.Unix() {
			return false
		}
		return true
	}
	return false
}

func (self *RedisACRCursor) CanJumpHour() bool {
	Min := self.Cursor.Minute()
	Sec := self.Cursor.Second()
	if (Min == 0) && (Sec == 0) {
		proposed_jump_time := self.Cursor.Add(time.Hour)
		if proposed_jump_time.Unix() > self.To.Unix() {
			return false
		}
		return true
	}
	return false
}

func (self *RedisACRCursor) CanJumpMinute() bool {
	Sec := self.Cursor.Second()
	if Sec == 0 {
		proposed_jump_time := self.Cursor.Add(time.Minute)
		if proposed_jump_time.Unix() > self.To.Unix() {
			return false
		}
		return true
	}
	return false
}

func (self *RedisACRCursor) CanJumpSecond() bool {
	proposed_jump_time := self.Cursor.Add(time.Second)
	if proposed_jump_time.Unix() > self.To.Unix() {
		return false
	}
	return true
}

func (self *RedisACRCursor) YearKey() string {
	return self.Cursor.Format("2006")
}

func (self *RedisACRCursor) MonthKey() string {
	return self.Cursor.Format("200601")
}

func (self *RedisACRCursor) DayKey() string {
	return self.Cursor.Format("20060102")
}

func (self *RedisACRCursor) HourKey() string {
	return self.Cursor.Format("2006010215")
}

func (self *RedisACRCursor) MinuteKey() string {
	return self.Cursor.Format("200601021504")
}

func (self *RedisACRCursor) SecondKey() string {
	return self.Cursor.Format("20060102150405")
}

func (self *RedisACRCursor) Year() int {
	return self.Cursor.Year()
}

func (self *RedisACRCursor) Month() int {
	return int(self.Cursor.Month())
}

func (self *RedisACRCursor) Day() int {
	return self.Cursor.Day()
}

func (self *RedisACRCursor) Hour() int {
	return self.Cursor.Hour()
}

func (self *RedisACRCursor) Minute() int {
	return self.Cursor.Minute()
}

func (self *RedisACRCursor) Second() int {
	return self.Cursor.Second()
}

func (self *RedisACRCursor) JumpYear() {
	self.Cursor = self.Cursor.AddDate(1, 0, 0)
}

func (self *RedisACRCursor) JumpMonth() {
	self.Cursor = self.Cursor.AddDate(0, 1, 0)
}

func (self *RedisACRCursor) JumpDay() {
	self.Cursor = self.Cursor.AddDate(0, 0, 1)
}

func (self *RedisACRCursor) JumpHour() {
	self.Cursor = self.Cursor.Add(time.Hour)
}

func (self *RedisACRCursor) JumpMinute() {
	self.Cursor = self.Cursor.Add(time.Minute)
}

func (self *RedisACRCursor) JumpSecond() {
	self.Cursor = self.Cursor.Add(time.Second)
}
