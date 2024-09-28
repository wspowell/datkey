package hash

import (
	"github.com/wspowell/datkey/hash/internal/hashtag"
)

type Slot uint16

const MaxHashSlot Slot = 16384

func ToSlot(key string) Slot {
	return Slot(hashtag.ToSlot(key))
}

type Range struct {
	Begin Slot
	End   Slot
}

func (self Range) Contains(testSlot Slot) bool {
	return self.Begin <= testSlot && testSlot <= self.End
}

// TODO: Not tested.
func (self Range) Overlaps(other Range) bool {
	return (other.Begin <= self.Begin && self.Begin <= other.End) ||
		(other.Begin <= self.End && self.End <= other.End) ||
		other.Overlaps(self)
}
