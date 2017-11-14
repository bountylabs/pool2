// A generic resource pool for databases etc
package pool

import (
	"errors"
	"io"
	"sync/atomic"
	"time"

	"github.com/bountylabs/api_common/errutil"
)

var (
	TimeoutError              = errors.New("Pool Timeout")
	PoolClosedError           = errors.New("Pool is closed")
	NewConnectionLimitedError = errors.New("Exceeded rate limit for creating new pool connections")
)

type Limiter interface {
	Allow() bool
}

// ResourceOpener opens a resource
type ResourceOpener interface {
	Open() (Resource, error)
}

type Resource interface {
	io.Closer
	// Good returns true when the resource is in good state
	Good() bool
}

// Ticket abstract type that represents the right to acquire a resource
type Ticket struct{}

type PooledResource interface {
	// Release releases the resource back to the pool for reuse
	Release() error
	// Destroy destroys the resource. It is no longer usable.
	Destroy() error
	// Resource returns the underlying Resource
	Resource() Resource
}

type PoolMetrics interface {
	ReportResources(stats ResourcePoolStat)
	ReportWait(wt time.Duration)       //how long did we wait for a connection
	ReportBorrowTime(wt time.Duration) //how long was the connection used for
	ReportNew(time time.Duration)      //how long did it take for a new connection to be opened
	ReportNewConnectionRateLimited()   // increment each time we don't create a new connection because we are rate limited
}

type ResourcePoolStat struct {
	AvailableNow  uint32
	ResourcesOpen uint32
	Cap           uint32
	InUse         uint32
}

// ResourcePool manages a pool of resources for reuse.
type ResourcePool struct {
	metrics PoolMetrics //metrics interface to track how the pool performs

	// To get a resource, get a ticket first and then get the resource from
	// either the reserve or open a new one.
	// To release a resource, first put the resource back to the reserve
	// and then put the ticket back.
	// The order is important to make sure we never create >cap(tickets)
	// number of resources.
	reserve chan Resource // idle resources, ready for use
	tickets chan *Ticket  // ticket to own resources

	opener               ResourceOpener
	closed               chan struct{}
	nOpenResources       uint32
	newConnectionLimiter Limiter
}

// NewPool creates a new pool of Clients.
func NewPool(maxReserve, maxOpen uint32, opener ResourceOpener, m PoolMetrics, newConnectionLimiter Limiter) *ResourcePool {
	if maxOpen < maxReserve {
		panic("maxOpen must be > maxReserve")
	}
	tickets := make(chan *Ticket, maxOpen)
	for i := uint32(0); i < maxOpen; i++ {
		tickets <- &Ticket{}
	}
	return &ResourcePool{
		metrics:              m,
		reserve:              make(chan Resource, maxReserve),
		tickets:              tickets,
		opener:               opener,
		closed:               make(chan struct{}),
		newConnectionLimiter: newConnectionLimiter,
	}
}

func (p *ResourcePool) Get() (PooledResource, error) {
	return p.GetWithTimeout(365 * 24 * time.Hour) // 1 year is forever
}

type pooledResource struct {
	ticket *Ticket
	served time.Time
	p      *ResourcePool
	res    Resource
}

func (pr *pooledResource) Release() error {
	defer pr.p.reportResources()
	pr.p.reportBorrowTime(time.Now().Sub(pr.served))
	return pr.p.release(pr)
}

func (pr *pooledResource) Destroy() error {
	defer pr.p.reportResources()
	pr.p.reportBorrowTime(time.Now().Sub(pr.served))
	return pr.p.destroy(pr)
}

func (pr *pooledResource) Resource() Resource {
	return pr.res
}

func (p *ResourcePool) releaseTicket(ticket *Ticket) {
	if ticket == nil {
		panic("BUG: can not release a nil ticket")
	}
	select {
	case p.tickets <- ticket:
	default:
		// releasing ticket should never block.
		panic("BUG: releaseTicket is called when ticket is not issued.")
	}
}

// WarmUp allocates resources until we have approximately allocated
// cap(p.reserve) resources.  We may under or over allocate resources (the code
// is subject to races) if someone is getting a resource while we are warming
// up.
func (p *ResourcePool) WarmUp() (count int, err error) {

	//loop until our open resources equals our reserve size
	for int(p.GetNOpenResources()) < cap(p.reserve) {

		var ticket *Ticket
		select {
		case ticket = <-p.tickets:
		case <-p.closed:
			return count, nil
		default: //all tickets are handed out
			return count, nil
		}

		if pooledResource, err := p.open(ticket, true); err != nil {
			return count, err
		} else {
			count++
			pooledResource.Release()
			continue
		}
	}

	return count, nil
}

func (p *ResourcePool) GetWithTimeout(timeout time.Duration) (pr PooledResource, err error) {

	// order is important: first ticket then reserve
	defer p.reportResources()
	defer func(start time.Time) {
		p.reportWait(time.Now().Sub(start))
	}(time.Now())

	timer := time.NewTimer(timeout)

	var ticket *Ticket
	select {
	case ticket = <-p.tickets:
	case <-timer.C:
		return nil, TimeoutError
	case <-p.closed:
		timer.Stop()
		return nil, PoolClosedError
	}
	timer.Stop()
	if p.isClosed() {
		// release ticket on close
		p.releaseTicket(ticket)
		return nil, PoolClosedError
	}

	if pooledResource := p.getFromReserve(ticket); pooledResource != nil {
		return pooledResource, nil
	}

	return p.open(ticket, false)

}

// getFromReserve attempts to get a pooledResource from the reserve pool, requires a ticket!
func (p *ResourcePool) getFromReserve(ticket *Ticket) *pooledResource {

	if ticket == nil {
		panic("BUG: can not acquire a reserved resource without a ticket")
	}

	for {
		select {
		case r := <-p.reserve:
			if r.Good() {
				return &pooledResource{ticket: ticket, served: time.Now(), p: p, res: r}
			} else {
				p.closeResource(r)
			}
			continue
		default:
			return nil
		}
	}
}

// open, opens a new resource, requires a ticket!
func (p *ResourcePool) open(ticket *Ticket, fromWarmup bool) (*pooledResource, error) {

	if ticket == nil {
		panic("BUG: can not open a resource without a ticket")
	}

	if !fromWarmup && p.newConnectionLimiter != nil {
		if !p.newConnectionLimiter.Allow() {
			p.reportNewConnectionRateLimited()
			return nil, NewConnectionLimitedError
		}
	}

	start := time.Now()
	r, err := p.opener.Open()
	p.reportNew(time.Now().Sub(start))

	if err != nil {
		// release ticket on error
		p.releaseTicket(ticket)
		return nil, err
	}
	p.incrOpen()
	return &pooledResource{ticket: ticket, served: time.Now(), p: p, res: r}, nil
}

func (p *ResourcePool) closeResource(res Resource) error {
	p.decrOpen()
	return res.Close()
}

func (p *ResourcePool) release(pr *pooledResource) error {
	var err error
	// order is important: first reserve then ticket
	res := pr.Resource()
	select {
	case p.reserve <- res:
	default:
		// reserve is full
		err = p.closeResource(res)
	}
	p.releaseTicket(pr.ticket)
	pr.ticket = nil

	if p.isClosed() {
		p.drainReserve()
	}
	return err
}

func (p *ResourcePool) destroy(pr *pooledResource) error {
	p.releaseTicket(pr.ticket)
	pr.ticket = nil
	return p.closeResource(pr.Resource())
}

// Close closes the pool. Resources in use are not affected.
func (p *ResourcePool) Close() error {
	close(p.closed)
	return p.drainReserve()
}

func (p *ResourcePool) isClosed() bool {
	select {
	case <-p.closed:
		return true
	default:
		return false
	}
}

func (p *ResourcePool) drainReserve() error {
	out := []error{}
	for {
		select {
		case r := <-p.reserve:
			if err := p.closeResource(r); err != nil {
				out = append(out, err)
			}
		default:
			return errutil.ErrSlice(out)
		}
	}
}

/**
Metrics
**/

func (p *ResourcePool) reportNew(time time.Duration) {
	if p.metrics != nil {
		p.metrics.ReportNew(time)
	}
}

func (p *ResourcePool) reportWait(wt time.Duration) {
	if p.metrics != nil {
		p.metrics.ReportWait(wt)
	}
}

func (p *ResourcePool) reportBorrowTime(wt time.Duration) {
	if p.metrics != nil {
		p.metrics.ReportBorrowTime(wt)
	}
}

func (p *ResourcePool) reportResources() {
	if p.metrics != nil {
		stats := p.Stats()
		p.metrics.ReportResources(stats)
	}
}

func (p *ResourcePool) reportNewConnectionRateLimited() {
	if p.metrics != nil {
		p.metrics.ReportNewConnectionRateLimited()
	}
}

func (p *ResourcePool) incrOpen() {
	atomic.AddUint32(&p.nOpenResources, 1)
}

func (p *ResourcePool) decrOpen() {
	atomic.AddUint32(&p.nOpenResources, ^uint32(0))
}

func (p *ResourcePool) GetNOpenResources() uint32 {
	return atomic.LoadUint32(&p.nOpenResources)
}

// Stats returns an inconsistent (approximate) view of the pool's metrics. EG:
// we may say there are more in use connections than are open.
func (p *ResourcePool) Stats() ResourcePoolStat {
	tot := uint32(cap(p.tickets))
	n := uint32(len(p.tickets))
	inuse := tot - n
	available := uint32(len(p.reserve))

	return ResourcePoolStat{
		AvailableNow:  available,
		ResourcesOpen: p.GetNOpenResources(),
		Cap:           tot,
		InUse:         inuse,
	}
}
