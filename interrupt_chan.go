package lmdbbs

// closeChan is a chan that is created only to be closed
// with no values to be sent
type closeChan chan struct{}

// interruptChan is a data type that allows to interrupt
// another goroutine and wait for that routine to confirm
// that it finished processing the interrupt signal
type interruptChan struct {
	intr chan closeChan
	done closeChan
}

// makeInterruptChan return new interruptChan
func makeInterruptChan() interruptChan {
	return interruptChan{
		intr: make(chan closeChan),
		done: make(closeChan),
	}
}

// InterruptChan returns internal interruption channel
// to be used in select
// It is only safe to use this chan from the context where it's
// known for sure that `interruptChan` is not closed
func (ch interruptChan) InterruptChan() <-chan closeChan {
	return ch.intr
}

// IsInterrupted returns nil if interruptChan is not interrupted
// and a channel which should be closed upon cleanup of resources
// otherwise
func (ch interruptChan) IsInterrupted() closeChan {
	select {
	case notify := <-ch.intr:
		return notify
	default:
		return nil
	}
}

func (ch interruptChan) Close() {
	close(ch.done)
}

func (ch interruptChan) Interrupt() {
	notify := make(closeChan)
	select {
	case ch.intr <- notify:
		<-notify
	case <-ch.done:
	}
}

func (ch interruptChan) IsClosed() bool {
	select {
	case <-ch.done:
		return true
	default:
		return false
	}
}
