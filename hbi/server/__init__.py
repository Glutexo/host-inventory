import uuid

from collections import defaultdict
from itertools import chain

from hbi.model import Host, Filter


def flat_fact_chain(f):
    return chain.from_iterable(v.items() for v in f.values())


class Index(object):

    def __init__(self):
        self.dict_ = {}
        self.all_hosts = set()
        self.account_dict = defaultdict(set)

    def add(self, host):
        if not isinstance(host, Host):
            msg = f"Index only stores Host objects, was given type {type(host)}"
            raise ValueError(msg)
        self.all_hosts.add(host)
        self.dict_[host.id] = host
        self.account_dict[host.account_number].add(host)
        for t in host.canonical_facts.items():
            self.dict_[t] = host
        # TODO: Actually USE the namespaces
        f_chain = flat_fact_chain(host.facts)
        t_chain = flat_fact_chain(host.tags)
        for t in chain(f_chain, t_chain):
            if t not in self.dict_:
                self.dict_[t] = set()
            self.dict_[t].add(host)

    def get(self, host):
        if host.id:
            return self.dict_.get(host.id)

        for t in host.canonical_facts.items():
            h = self.dict_.get(t)
            if h:
                return h

    def apply_filter(self, f, hosts=None):
        if hosts is None:
            hosts = self.all_hosts
        elif len(hosts) == 0:
            raise StopIteration

        if f.account_numbers:
            for acct in f.account_numbers:
                yield from self.account_dict[acct]

        # TODO: Actually USE the fact & tag namespaces
        queries = chain(
            f.ids,
            f.canonical_facts.items(),
            flat_fact_chain(f.facts),
            flat_fact_chain(f.tags)
        )
        for i in queries:
            v = self.dict_.get(i)
            if type(v) == set:
                yield from (i for i in v if i in hosts)
            elif v in hosts:
                yield v

    # orig *has* to be from a `get` to be safe
    def merge(self, orig, new):
        for t in orig.canonical_facts.items():
            del self.dict_[t]

        # TODO: update index dict for facts and tags
        orig.merge(new)

        for t in orig.canonical_facts.items():
            self.dict_[t] = orig


class Service(object):
    def __init__(self):
        self.index = Index()

    def reset(self):
        self.index = Index()

    def create_or_update(self, hosts):
        ret = []
        for h in hosts:
            if h.canonical_facts is None and h.id is None:
                raise ValueError("Must provide at least one canonical fact or the ID")

            # search the canonical_facts dict for a match
            existing_host = self.index.get(h)
            if existing_host:
                self.index.merge(existing_host, h)
            else:  # Host not found.  Create it.
                existing_host = h
                existing_host.id = uuid.uuid4().hex
                self.index.add(h)

            ret.append(existing_host)

        return ret

    def get(self, filters=None):
        if not filters:
            return list(self.index.all_hosts)
        elif type(filters) != list or any(type(f) != Filter for f in filters):
            raise ValueError("Query must be a list of Filter objects")
        else:
            filtered_set = None
            for f in filters:
                filtered_set = set(self.index.apply_filter(f, filtered_set))
                # If we have no results, we'll never get more so exit now.
                if len(filtered_set) == 0:
                    return []
            return list(filtered_set)
