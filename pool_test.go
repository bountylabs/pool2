package pool

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

type fakeResource struct {
	bad bool
}

func (r *fakeResource) Close() error {
	return nil
}

func (r *fakeResource) Good() bool {
	return !r.bad
}

type fakeResourceOpener struct {
	id       int32
	failOpen bool
}

func (ro *fakeResourceOpener) Open() (Resource, error) {
	if ro.failOpen {
		return nil, errors.New("open failed")
	}
	atomic.AddInt32(&ro.id, 1)
	return &fakeResource{}, nil
}

func (ro *fakeResourceOpener) ID() int {
	return int(atomic.LoadInt32(&ro.id))
}

func TestBasic(t *testing.T) {
	opener := fakeResourceOpener{}

	p := NewPool(1, 2, &opener, nil)

	Convey("get a resource", t, func() {
		pr, err := p.Get()
		So(err, ShouldBeNil)
		So(opener.ID(), ShouldEqual, 1)
		pr.Release()
		// Bug in convey, this should be ShouldEqual
		So(p.Stats(), ShouldResemble, ResourcePoolStat{
			AvailableNow:  1,
			ResourcesOpen: 1,
			Cap:           2,
			InUse:         0,
		})

		Convey("get a resource in reserve", func() {
			pr, err := p.Get()
			So(err, ShouldBeNil)
			So(opener.ID(), ShouldEqual, 1)
			So(p.Stats(), ShouldResemble, ResourcePoolStat{
				AvailableNow:  0,
				ResourcesOpen: 1,
				Cap:           2,
				InUse:         1,
			})

			Convey("only one reserve", func() {
				pr2, err := p.Get()
				So(err, ShouldBeNil)
				So(opener.ID(), ShouldEqual, 2)
				So(p.Stats(), ShouldResemble, ResourcePoolStat{
					AvailableNow:  0,
					ResourcesOpen: 2,
					Cap:           2,
					InUse:         2,
				})

				pr.Release()
				pr2.Release()
				So(p.Stats(), ShouldResemble, ResourcePoolStat{
					AvailableNow:  1,
					ResourcesOpen: 1,
					Cap:           2,
					InUse:         0,
				})
			})
		})
	})

	Convey("destroy a resource", t, func() {
		pr, err := p.Get()
		So(err, ShouldBeNil)
		So(opener.ID(), ShouldEqual, 2)
		So(p.Stats(), ShouldResemble, ResourcePoolStat{
			AvailableNow:  0,
			ResourcesOpen: 1,
			Cap:           2,
			InUse:         1,
		})

		pr.Destroy()
		So(p.Stats(), ShouldResemble, ResourcePoolStat{
			AvailableNow:  0,
			ResourcesOpen: 0,
			Cap:           2,
			InUse:         0,
		})
	})

	Convey("get times out", t, func() {
		pr, err := p.Get()
		So(err, ShouldBeNil)
		So(opener.ID(), ShouldEqual, 3)

		pr2, err := p.Get()
		So(err, ShouldBeNil)
		So(opener.ID(), ShouldEqual, 4)
		So(p.Stats(), ShouldResemble, ResourcePoolStat{
			AvailableNow:  0,
			ResourcesOpen: 2,
			Cap:           2,
			InUse:         2,
		})

		done := make(chan struct{}, 1)
		go func() {
			pr3, err := p.GetWithTimeout(5 * time.Second)
			if err != nil {
				t.Fatalf("GetWithTimeout failed=%v", err)
			}
			done <- struct{}{}
			pr3.Destroy()
		}()

		select {
		case <-done:
			So(true, ShouldBeFalse)
		case <-time.After(20 * time.Millisecond):
		}

		pr.Release()
		select {
		case <-done:
		case <-time.After(20 * time.Millisecond):
			So(true, ShouldBeFalse)
		}
		So(opener.ID(), ShouldEqual, 4)
		So(p.Stats(), ShouldResemble, ResourcePoolStat{
			AvailableNow:  0,
			ResourcesOpen: 1,
			Cap:           2,
			InUse:         1,
		})

		pr2.Destroy()
		So(p.Stats(), ShouldResemble, ResourcePoolStat{
			AvailableNow:  0,
			ResourcesOpen: 0,
			Cap:           2,
			InUse:         0,
		})
	})

	Convey("open resource failure", t, func() {
		opener.failOpen = true
		defer func() { opener.failOpen = false }()

		pr, err := p.Get()
		So(err, ShouldNotBeNil)
		So(pr, ShouldBeNil)
		So(p.Stats(), ShouldResemble, ResourcePoolStat{
			AvailableNow:  0,
			ResourcesOpen: 0,
			Cap:           2,
			InUse:         0,
		})
	})

	Convey("a bad resource in reserve is not returned", t, func() {
		pr, err := p.Get()
		So(err, ShouldBeNil)
		So(opener.ID(), ShouldEqual, 5)
		pr.Release()
		pr.Resource().(*fakeResource).bad = true
		So(p.Stats(), ShouldResemble, ResourcePoolStat{
			AvailableNow:  1,
			ResourcesOpen: 1,
			Cap:           2,
			InUse:         0,
		})

		pr2, err := p.Get()
		So(err, ShouldBeNil)
		So(opener.ID(), ShouldEqual, 6)
		pr2.Destroy()
		So(p.Stats(), ShouldResemble, ResourcePoolStat{
			AvailableNow:  0,
			ResourcesOpen: 0,
			Cap:           2,
			InUse:         0,
		})
	})

	Convey("close pool", t, func() {
		pr, err := p.Get()
		So(err, ShouldBeNil)
		pr2, err := p.Get()
		So(err, ShouldBeNil)
		So(opener.ID(), ShouldEqual, 8)
		So(p.Stats(), ShouldResemble, ResourcePoolStat{
			AvailableNow:  0,
			ResourcesOpen: 2,
			Cap:           2,
			InUse:         2,
		})

		pr.Release()
		So(p.Stats(), ShouldResemble, ResourcePoolStat{
			AvailableNow:  1,
			ResourcesOpen: 2,
			Cap:           2,
			InUse:         1,
		})

		Convey("close does not affect resources in use", func() {
			err = p.Close()
			So(err, ShouldBeNil)
			So(p.Stats(), ShouldResemble, ResourcePoolStat{
				AvailableNow:  0,
				ResourcesOpen: 1,
				Cap:           2,
				InUse:         1,
			})
			So(p.isClosed(), ShouldBeTrue)

			Convey("get fails after pool is closed", func() {
				pr, err := p.Get()
				So(err, ShouldNotBeNil)
				So(pr, ShouldBeNil)
				So(p.Stats(), ShouldResemble, ResourcePoolStat{
					AvailableNow:  0,
					ResourcesOpen: 1,
					Cap:           2,
					InUse:         1,
				})

				Convey("no reserve after pool is closed", func() {
					err := pr2.Release()
					So(err, ShouldBeNil)
					So(p.Stats(), ShouldResemble, ResourcePoolStat{
						AvailableNow:  0,
						ResourcesOpen: 0,
						Cap:           2,
						InUse:         0,
					})
				})
			})
		})

	})
}

// TODO(binz): add a random test to exercise ResourcePool more
// - have goroutines call Get, GetWithTimeout, and Close randomly.
// - after all resources are released, verify the stats of all Pools ever created.
