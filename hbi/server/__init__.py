import uuid

from collections import defaultdict
from itertools import chain

from hbi.model import Host, Filter


def facts_to_lookup_keys(facts):
    """
    Converts a dict of facts/tags dicts into a chain of key-value
    tuples, throwing away the namespaces being the topmost keys.
    """
    return chain.from_iterable(v.items() for v in facts.values())


class Index(object):
    """
    In-memory database-like storage using a native Python dict as a
    look-up table.
    """

    def __init__(self):
        self.all_hosts = set()  # Default unfiltered result set

        # Look-up tables
        self.dict = {}
        self.account_dict = defaultdict(set)

    def add(self, host):
        """
        Add a new host to the database.
        """
        if not isinstance(host, Host):
            msg = f"Index only stores Host objects, was given type {type(host)}"
            raise ValueError(msg)

        self.all_hosts.add(host)

        # Look-ups that return a single host
        self.dict[host.id] = host
        self.account_dict[host.account_number].add(host)
        for t in host.canonical_facts.items():
            self.dict[t] = host

        # Look-ups that return multiple hosts
        # TODO: Actually USE the namespaces
        facts_keys = facts_to_lookup_keys(host.facts)
        tags_keys = facts_to_lookup_keys(host.tags)
        for t in chain(facts_keys, tags_keys):
            if t not in self.dict:
                self.dict[t] = set()
            self.dict[t].add(host)

    def get(self, host):
        """
        Get a host by its id or any canonical fact.
        """
        if host.id:
            return self.dict.get(host.id)

        for lookup_key in host.canonical_facts.items():
            h = self.dict.get(lookup_key)
            if h:
                return h

    def apply_filter(self, f, hosts=None):
        """
        Filter given (or all) hosts.
        """

        if hosts is None:
            hosts = self.all_hosts
        elif len(hosts) == 0:
            raise StopIteration

        # TODO: Actually USE the fact & tag namespaces
        lookup_keys_iterables = filter(None, (
            f.ids, f.canonical_facts.items(),
            facts_to_lookup_keys(f.facts),
            facts_to_lookup_keys(f.tags)
        ))

        if f.account_numbers:
            for acct in f.account_numbers:
                yield from self.account_dict[acct]

        for lookup_key in chain(*lookup_keys_iterables):
            v = self.dict.get(lookup_key)
            if type(v) == set:
                yield from (host for host in v if host in hosts)
            elif v in hosts:
                yield v

    # orig *has* to be from a `get` to be safe
    def merge(self, existing_host, new_host):
        """
        Merge data from a new host to an existing host.
        """
        # Delete all original canonical facts lookup keys, so the host
        # can be found only with the new ones.
        for lookup_key in existing_host.canonical_facts.items():
            del self.dict[lookup_key]

        # TODO: update index dict for facts and tags
        existing_host.merge(new_host)

        for lookup_key in existing_host.canonical_facts.items():
            self.dict[lookup_key] = existing_host


class Service(object):
    """
    An interface for the host database.
    """

    def __init__(self):
        """
        Creates a new in-memory database. (Like setting up a new RDBMS
        connection.)
        """
        self.index = Index()

    def reset(self):
        """
        Throw away the current in-memory database and create a new one.
        (Like rolling back pending RDBMS transactions.)
        """
        self.index = Index()

    def create_or_update(self, hosts):
        """
        Saves a new host data to the database: either creates a new host
        or updates an existing one.
        """
        ret = []
        for h in hosts:
            if h.canonical_facts is None and h.id is None:
                raise ValueError("Must provide at least one canonical fact or the ID")

            # Search the look-up table for a match
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
        """
        Gets all hosts that match given filters. All hosts if no filters
        are given.
        """
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
