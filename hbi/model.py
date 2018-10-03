from collections import defaultdict

from hbi import hbi_pb2


def to_fact_pb(ft, canonical=False):
    if canonical:
        return [hbi_pb2.CanonicalFact(key=k, value=v)
                for k, v in ft.items()]
    else:
        return [hbi_pb2.Fact(namespace=namespace, key=k, value=v)
                for namespace, facts in ft.items()
                for k, v in facts.items()]


def from_fact_pb(ft):
    d = defaultdict(dict)
    if ft:
        for fact in ft:
            d[fact.namespace][fact.key] = fact.value
    return d


class Filter(object):

    def __init__(self, canonical_facts=None, ids=None, account_numbers=None,
                 tags=None, facts=None):
        self.ids = ids or set()
        self.canonical_facts = canonical_facts or {}
        self.tags = tags or defaultdict(dict)
        self.facts = facts or defaultdict(dict)
        self.account_numbers = account_numbers or set()

    @classmethod
    def from_pb(cls, filter_):
        return cls(
            {f.key: f.value for f in filter_.canonical_facts},
            filter_.ids,
            filter_.account_numbers,
            from_fact_pb(filter_.tags),
            from_fact_pb(filter_.facts),
        )

    def to_pb(self):
        return hbi_pb2.Filter(
            ids=self.ids,
            canonical_facts=to_fact_pb(self.canonical_facts, canonical=True),
            account_numbers=self.account_numbers,
            facts=to_fact_pb(self.facts),
            tags=to_fact_pb(self.tags)
        )

    @classmethod
    def from_json(cls, d):
        return cls(
            d.get("canonical_facts"),
            d.get("ids"),
            d.get("account_numbers"),
            d.get("tags"),
            d.get("facts")
        )

    def to_json(self):
        return {
            "canonical_facts": self.canonical_facts,
            "ids": self.ids,
            "account_numbers": self.account_numbers,
            "tags": self.tags,
            "facts": self.facts
        }


class Host(object):

    def __init__(self, canonical_facts, id_=None, account_number=None,
                 display_name=None, tags=None, facts=None):
        self.id = id_
        self.canonical_facts = canonical_facts
        self.display_name = display_name
        self.tags = tags or defaultdict(dict)
        self.facts = facts or defaultdict(dict)
        self.account_number = account_number

    @classmethod
    def from_pb(cls, host):
        return cls(
            {f.key: f.value for f in host.canonical_facts},
            host.id,
            host.account_number,
            host.display_name,
            from_fact_pb(host.tags),
            from_fact_pb(host.facts),
        )

    @classmethod
    def from_json(cls, d):
        return cls(
            d.get("canonical_facts"),
            d.get("id"),
            d.get("account_number"),
            d.get("display_name"),
            d.get("tags"),
            d.get("facts")
        )

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id

    def __str__(self):
        return f"{self.id} -> {self.canonical_facts}; {self.facts}"

    def to_pb(self):
        facts = to_fact_pb(self.facts)
        canonical_facts = to_fact_pb(self.canonical_facts, canonical=True)
        tags = to_fact_pb(self.tags)

        return hbi_pb2.Host(id=self.id,
                            account_number=self.account_number,
                            display_name=self.display_name,
                            canonical_facts=canonical_facts,
                            tags=tags,
                            facts=facts)

    def to_json(self):
        return {
            "canonical_facts": self.canonical_facts,
            "id": self.id,
            "account_number": self.account_number,
            "display_name": self.display_name,
            "tags": self.tags,
            "facts": self.facts
        }

    def merge(self, new):
        for k, v in new.canonical_facts.items():
            self.canonical_facts[k] = v

        for namespace, d in new.facts.items():
            self.facts[namespace] = d

        for namespace, d in new.tags.items():
            self.tags[namespace] = d

        self.display_name = new.display_name
