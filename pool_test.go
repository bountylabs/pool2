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

	p := NewPool(1, 2, &opener, nil, nil)

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

func TestWarmup(t *testing.T) {

	Convey("given a pool", t, func() {

		opener := fakeResourceOpener{}
		p := NewPool(2, 4, &opener, nil, nil)

		Convey("warm the pool up", func() {

			count, err := p.WarmUp()
			So(err, ShouldBeNil)
			So(count, ShouldEqual, 2)
			So(p.Stats(), ShouldResemble, ResourcePoolStat{
				AvailableNow:  2,
				ResourcesOpen: 2,
				Cap:           4,
				InUse:         0,
			})
		})

		Convey("warm the pool up after getting a connection", func() {

			pr, err := p.Get()
			So(err, ShouldBeNil)
			So(pr, ShouldNotBeNil)
			So(p.Stats(), ShouldResemble, ResourcePoolStat{
				AvailableNow:  0,
				ResourcesOpen: 1,
				Cap:           4,
				InUse:         1,
			})

			count, err := p.WarmUp()
			So(err, ShouldBeNil)
			So(count, ShouldEqual, 1)
			So(p.Stats(), ShouldResemble, ResourcePoolStat{
				AvailableNow:  1,
				ResourcesOpen: 2,
				Cap:           4,
				InUse:         1,
			})

			pr.Release()
			So(p.Stats(), ShouldResemble, ResourcePoolStat{
				AvailableNow:  2,
				ResourcesOpen: 2,
				Cap:           4,
				InUse:         0,
			})
		})

	})
}

type MockLimiter struct {
	allow bool
}

func (this *MockLimiter) Allow() bool {
	return this.allow
}

func TestNewConnectLimit(t *testing.T) {
	Convey("given a pool", t, func() {

		opener := fakeResourceOpener{}
		mockLimiter := &MockLimiter{allow: false}
		p := NewPool(1, 1, &opener, nil, mockLimiter)

		Convey("should not use limiter for warmup ", func() {
			count, err := p.WarmUp()
			So(err, ShouldBeNil)
			So(count, ShouldEqual, 1)
			So(p.Stats(), ShouldResemble, ResourcePoolStat{
				AvailableNow:  1,
				ResourcesOpen: 1,
				Cap:           1,
				InUse:         0,
			})

			Convey("should prevent creation of too many new connections", func() {
				// Should be fine since it reuses connection created during warmup
				pr, err := p.GetWithTimeout(10 * time.Second)
				So(err, ShouldBeNil)

				// Mark as bad so next call force creating a new connection
				pr.Release()
				pr.Resource().(*fakeResource).bad = true

				mockLimiter.allow = true

				// Should be fine since limiter still allows it
				pr, err = p.GetWithTimeout(10 * time.Second)
				So(err, ShouldBeNil)

				So(p.Stats(), ShouldResemble, ResourcePoolStat{
					AvailableNow:  0,
					ResourcesOpen: 1,
					Cap:           1,
					InUse:         1,
				})

				mockLimiter.allow = false

				// Mark as bad so next call force creating a new connection
				pr.Release()
				pr.Resource().(*fakeResource).bad = true

				// Should error now that limiter prevents it
				pr, err = p.GetWithTimeout(10 * time.Second)
				So(err, ShouldEqual, NewConnectionLimitedError)

				So(p.Stats(), ShouldResemble, ResourcePoolStat{
					AvailableNow:  0, // Current connection is bad, so 0 available
					ResourcesOpen: 0, // Current connection is bad, so 0 open
					Cap:           1,
					InUse:         0, // When rate limited, we shouldn't say any are in use
				})

				// Once limiter allows getting a new connection, ensure it can be created
				mockLimiter.allow = true
				pr, err = p.GetWithTimeout(10 * time.Second)
				So(err, ShouldBeNil)

				So(p.Stats(), ShouldResemble, ResourcePoolStat{
					AvailableNow:  0,
					ResourcesOpen: 1,
					Cap:           1,
					InUse:         1,
				})
			})
		})
	})
}

// TODO(binz): add a random test to exercise ResourcePool more
// - have goroutines call Get, GetWithTimeout, and Close randomly.
// - after all resources are released, verify the stats of all Pools ever created.
