// A generic resource pool for databases etc
package pool

import (
	"errors"
	"time"

	"github.com/bountylabs/pool"
)

type resourceOpen func() (interface{}, error)
type resourceClose func(interface{})
type resourceTest func(interface{}) error

var ResourceCreationError = errors.New("Resource Creation Failed")
var ResourceExhaustedError = errors.New("Pool Exhausted")
var ResourceTestError = errors.New("Resource Test Failed")
var Timeout = errors.New("Timeout")
var PoolClosedError = errors.New("Pool is closed")

type resourceWrapper struct {
	r interface{}
	p *ResourcePool
	t *int
}

func (rw resourceWrapper) Close() {
	rw.p.release(&rw)
}

func (rw resourceWrapper) Destroy() {
	rw.p.destroy(&rw)
}

func (rw resourceWrapper) Resource() interface{} {
	return rw.r
}

type ResourcePoolWrapper interface {
	Close()
	Destroy()
	Resource() interface{}
}

type PoolMetrics interface {
	ReportResources(stats ResourcePoolStat)
	ReportWait(wt time.Duration)
}

type ResourcePoolStat struct {
	AvailableNow  uint32
	ResourcesOpen uint32
	Cap           uint32
	InUse         uint32
}

type ResourcePool struct {
	metrics pool.PoolMetrics //metrics interface to track how the pool performs
	timeout time.Duration    //when aquiring a resource, how long should we wait before timining out

	reserve chan *resourceWrapper //channel of available resources
	tickets chan *int             //channel of available tickets to create a resource

	//callbacks for opening, testing and closing a resource
	resOpen  func() (interface{}, error)
	resClose func(interface{}) //we can't do anything with a close error
	resTest  func(interface{}) error
}

// NewPool creates a new pool of Clients.
func NewPool(
	maxReserve uint32,
	maxOpen uint32,
	o resourceOpen,
	c resourceClose,
	t resourceTest,
	m pool.PoolMetrics,
) *ResourcePool {

	if maxOpen < maxReserve {
		panic("maxOpen must be > maxReserve")
	}

	//create the pool
	p := &ResourcePool{
		reserve:  make(chan *resourceWrapper, maxReserve),
		tickets:  make(chan *int, maxOpen),
		resOpen:  o,
		resClose: c,
		resTest:  t,
		timeout:  time.Second,
		metrics:  m,
	}

	//create a ticket for each possible open resource
	for i := 0; i < int(maxOpen); i++ {
		p.tickets <- &i
	}

	return p
}

func (p *ResourcePool) Get() (resource ResourcePoolWrapper, err error) {
	return p.GetWithTimeout(p.timeout)
}

func (p *ResourcePool) GetWithTimeout(timeout time.Duration) (resource ResourcePoolWrapper, err error) {

	start := time.Now()

	for {

		if time.Now().After(start.Add(timeout)) {
			return nil, Timeout
		}

		r, e := p.getAvailable()

		//if the test failed try again
		if e == ResourceTestError {
			time.Sleep(time.Microsecond)
			continue
		}

		//if we are at our max open try again after a short sleep
		if e == ResourceExhaustedError {
			time.Sleep(time.Microsecond)
			continue
		}

		//if we failed to create a new resource, try agaig after a short sleep
		if e == ResourceCreationError {
			time.Sleep(time.Microsecond)
			continue
		}

		p.reportWait(time.Now().Sub(start))
		return r, e
	}

}

// Borrow a Resource from the pool, create one if we can
func (p *ResourcePool) getAvailable() (*resourceWrapper, error) {
	select {
	case r, ok := <-p.reserve:

		if ok == false {
			return nil, PoolClosedError
		}

		//test that the re-used resource is still good
		if err := p.resTest(r.r); err != nil {
			return nil, ResourceTestError
		}

		return r, nil
	default:
	}

	//nothing in reserve
	return p.openNewResource()

}

func (p *ResourcePool) openNewResource() (*resourceWrapper, error) {

	select {

	//aquire a ticket to open a resource
	case ticket, ok := <-p.tickets:

		if ok == false {
			return nil, PoolClosedError
		}

		obj, err := p.resOpen()

		//if the open fails, return our ticket
		if err != nil {

			//if the pool is closed let the ticket go
			select {
			case p.tickets <- ticket:
			default:
			}

			return nil, ResourceCreationError
		}

		return &resourceWrapper{p: p, t: ticket, r: obj}, nil

	//if we couldn't get a ticket we have hit our max number of resources
	default:
		return nil, ResourceExhaustedError
	}

}

// Return returns a Resource to the pool.
func (p *ResourcePool) release(r *resourceWrapper) {

	//put the resource back in the cache
	select {
	case p.reserve <- r:
	default:

		//the reserve is full, close the resource and put our ticket back
		p.resClose(r.r)

		//if tickets is closed, whatever
		select {
		case p.tickets <- r.t:
		default:
		}
	}
}

// Removes a Resource
func (p *ResourcePool) destroy(r *resourceWrapper) {

	p.resClose(r.r)

	//if tickets are closed, whatever
	select {
	case p.tickets <- r.t:
	default:
	}
}

func (p *ResourcePool) Close() {

	p.drainReserve()
	p.drainTickets()
}

func (p *ResourcePool) drainTickets() {

	for {
		select {
		case _ = <-p.tickets:
		default:
			close(p.tickets)
			return
		}
	}
}

func (p *ResourcePool) drainReserve() {

	for {
		select {
		case resource := <-p.reserve:
			p.resClose(resource.r)
		default:
			close(p.reserve)
			return
		}
	}
}

/**
Metrics
**/
func (p *ResourcePool) reportWait(d time.Duration) {
	if p.metrics != nil {
		go p.metrics.ReportWait(d)
		go p.metrics.ReportResources(p.Stats())
	}
}

func (p *ResourcePool) Stats() pool.ResourcePoolStat {

	open := uint32(cap(p.tickets) - len(p.tickets))
	available := uint32(len(p.reserve))

	return pool.ResourcePoolStat{
		AvailableNow:  available,
		ResourcesOpen: open,
		Cap:           uint32(cap(p.tickets)),
		InUse:         open - available,
	}
}
