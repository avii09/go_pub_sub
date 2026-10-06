package broker

// Topic is a named channel that clients subscribe to. It's not safe for
// concurrent use on its own: the Broker's mutex guards every Topic.
type Topic struct {
	name string
	subs map[*Client]struct{}
}

func newTopic(name string) *Topic {
	return &Topic{name: name, subs: make(map[*Client]struct{})}
}

func (t *Topic) add(c *Client)    { t.subs[c] = struct{}{} }
func (t *Topic) remove(c *Client) { delete(t.subs, c) }
func (t *Topic) empty() bool      { return len(t.subs) == 0 }

// snapshot copies the subscriber list so the caller can deliver messages
// after releasing the lock. Holding the lock while writing to slow sockets
// would stop anyone else from subscribing or unsubscribing.
func (t *Topic) snapshot() []*Client {
	out := make([]*Client, 0, len(t.subs))
	for c := range t.subs {
		out = append(out, c)
	}
	return out
}
