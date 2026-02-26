package offsetconsumer

// CommitFuture represents a pending asynchronous commit operation.
type CommitFuture interface {
	// Ok returns a channel that is closed when the commit succeeds.
	Ok() <-chan struct{}

	// Err returns a channel that receives the error if the commit fails.
	Err() <-chan error
}

type commitFuture struct {
	okCh  chan struct{}
	errCh chan error
}

func newCommitFuture() *commitFuture {
	return &commitFuture{
		okCh:  make(chan struct{}),
		errCh: make(chan error, 1),
	}
}

func (f *commitFuture) Ok() <-chan struct{} {
	return f.okCh
}

func (f *commitFuture) Err() <-chan error {
	return f.errCh
}
