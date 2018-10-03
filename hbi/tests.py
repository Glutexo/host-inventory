import os

from hbi.server import Host, Index, Filter, Service
from hbi.util import names
from pytest import fixture

MODE = os.environ.get("MODE", "").lower()


if MODE == "grpc":
    import grpc
    from hbi.server.grpc_server import serve
    from hbi.client import Client

if MODE == "tornado":
    from hbi.client import TornadoClient
    from hbi.server.tornado_server import serve_tornado
    # Starting one instance of tornado application for the whole suite
    # TODO: I tried to get it to stop the loop thread after tests are finished,
    # but it's still not working.
    app, loop = serve_tornado()


@fixture
def service():
    if MODE == "grpc":
        server = serve()
        connect_str = f"localhost:{os.environ.get('PORT', '50051')}"
        with grpc.insecure_channel(connect_str) as ch:
            yield Client(channel=ch)
        server.stop(0)
    elif MODE == "tornado":
        app.service.reset()
        yield TornadoClient()
    else:
        yield Service()


@fixture
def index():
    return Index()


@fixture
def host_list():
    return [Host({"hostname": n}, display_name=f"{n}.com")
            for n in ("-".join(dn) for dn in names())]


def test_create(service, host_list):
    ret_hostnames = {h.canonical_facts["hostname"]
                     for h in service.create_or_update(host_list)}
    original_hostnames = {h.canonical_facts["hostname"] for h in host_list}
    assert ret_hostnames == original_hostnames


def test_update(service):
    host = Host({
        "insights_id": "1234",
        "hostname": "inventory-test.redhat.com"
    })

    service.create_or_update([host])

    host = Host({
        "hostname": "inventory-test.redhat.com"
    }, facts={
        "advisor": {"cpu.count": "4"}
    })

    def validate(ret):
        assert ret.facts["advisor"]["cpu.count"] == "4"
        assert ret.canonical_facts["hostname"] == "inventory-test.redhat.com"
        assert ret.canonical_facts["insights_id"] == "1234"

    host = service.create_or_update([host])[0]
    validate(host)
    validate(service.get([Filter(ids=[host.id])])[0])


def test_get_all(service, host_list):
    service.create_or_update(host_list)
    ret_hosts = service.get()
    assert len(ret_hosts) == len(host_list)
    assert set(r.display_name for r in ret_hosts) == set(h.display_name for h in host_list)


def test_create_and_get(service, host_list):
    r = service.create_or_update(host_list)
    ret_hosts = service.get([Filter(ids=[r[0].id])])
    assert len(ret_hosts) == 1


def test_get_one(service, host_list):
    filters = Filter(ids=[service.create_or_update(host_list)[0].id])
    assert len(service.get([filters])) == 1


@fixture
def mf_hosts():
    return [
        Host(
            display_name='test1.example.org',
            canonical_facts={'insights_uuid': '11223344-5566-7788-99AA-BBCCDDEEFF00'},
            facts={'advisor': {'role': 'host'}},
            account_number='1234567',
        ),
        Host(
            display_name='test2.example.org',
            canonical_facts={'insights_uuid': '11223344-5566-7788-99AA-BBCCDDEEFF11'},
            facts={'advisor': {'role': 'manager'}},
            account_number='1234567',
        ),
        Host(
            display_name='test1.example.net',
            canonical_facts={'insights_uuid': '11223344-5566-7788-99AA-BBCCDDEEFF22'},
            facts={'advisor': {'role': 'host'}},
            account_number='1122334',
        ),
        Host(
            display_name='test2.example.net',
            canonical_facts={'insights_uuid': '11223344-5566-7788-99AA-BBCCDDEEFF33'},
            facts={'advisor': {'role': 'manager'}},
            account_number='1122334',
        ),
    ]


@fixture
def mfs(service, mf_hosts):
    """Multiple Filter Service"""
    hosts_added = service.create_or_update(mf_hosts)
    assert isinstance(hosts_added, list)
    assert len(mf_hosts) == len(hosts_added)
    for hostnum in range(len(mf_hosts)):
        # Compare pb2 objects directly
        assert hosts_added[hostnum].display_name == mf_hosts[hostnum].display_name
        assert hosts_added[hostnum].canonical_facts == mf_hosts[hostnum].canonical_facts
        assert hosts_added[hostnum].facts == mf_hosts[hostnum].facts
    return service


def test_one_hosts_single_id(mfs, mf_hosts):
    """We should be able to get one host by a single filter on ID."""
    hosts_added = mfs.get()
    hf = mfs.get(filters=[
        Filter(ids=[hosts_added[0].id])
    ])
    assert isinstance(hf, list)
    assert len(hf) == 1
    assert hf[0].display_name in [h.display_name for h in mf_hosts]


def test_one_host_one_fact(mfs):
    """
    We should be able to get one host by a single filter
    on canonical fact.
    """
    hf = mfs.get(filters=[
        Filter(canonical_facts={
            'insights_uuid': '11223344-5566-7788-99AA-BBCCDDEEFF11'
        })
    ])
    assert isinstance(hf, list)
    assert len(hf) == 1


def test_multiple_hosts_one_account(mfs, mf_hosts):
    """
    We should be able to get multiple hosts by a single
    filter on account.
    """
    hf = mfs.get(filters=[
        Filter(account_numbers=['1122334'])
    ])
    assert isinstance(hf, list)
    assert len(hf) == 2
    # Hosts are returned in random order, so check sorted lists
    assert sorted(h.display_name for h in hf) == sorted(h.display_name for h in mf_hosts[2:4])


def test_one_host_multiple_filters(mfs, mf_hosts):
    """
    We should be able to get a single host by multiple filters on account facts.
    Separate filters AND together - intersection of sets.
    """
    hf = mfs.get(filters=[
        Filter(account_numbers=['1122334']),
        Filter(facts={'advisor': {'role': 'host'}})
    ])
    assert isinstance(hf, list)
    assert len(hf) == 1
    assert hf[0].display_name == mf_hosts[2].display_name


def test_one_host_account_and_uuid(mfs, mf_hosts):
    """
    We should be able to get a single host by multiple filters on facts and
    canonical_facts.
    """
    hf = mfs.get(filters=[
        Filter(account_numbers=['1234567']),
        Filter(canonical_facts={
            'insights_uuid': '11223344-5566-7788-99AA-BBCCDDEEFF11',
        })
    ])
    assert isinstance(hf, list)
    assert len(hf) == 1
    assert hf[0].display_name == mf_hosts[1].display_name


def test_no_hosts_multiple_filters(mfs, mf_hosts):
    """When multiple filters have no intersection, we should get nothing"""
    hf = mfs.get(filters=[
        Filter(account_numbers=['1122334']),
        Filter(canonical_facts={
            'insights_uuid': '11223344-5566-7788-99AA-BBCCDDEEFF11'
        })
    ])
    assert isinstance(hf, list)
    assert len(hf) == 0


def test_multiple_hosts_and_facts_one_filter(mfs, mf_hosts):
    """
    We should be able to get multiple hosts with a single filter that
    looks for two separate account facts.
    In the same filter, facts OR together - union of sets.
    """
    hf = mfs.get(filters=[
        Filter(facts={'advisor': {'role': 'manager'}}, account_numbers=['1122334'])
    ])
    assert isinstance(hf, list)
    assert len(hf) == 3
    # Hosts are returned in random order, so check sorted lists
    assert sorted(h.display_name for h in hf) == sorted(h.display_name for h in mf_hosts[1:4])


def test_one_filter_takes_out_all(mfs, mf_hosts):
    """
    If one filter matches something that doesn't exist, and there are
    multiple filters, we should still get nothing (canonical fact first)
    """
    hf = mfs.get(filters=[
        Filter(canonical_facts={
            'insights_uuid': '11223344-5566-7788-99AA-000000000000'
        }),
        Filter(account_numbers=['1122334']),
    ])
    assert isinstance(hf, list)
    assert len(hf) == 0


def test_one_filter_takes_out_all_reverse_order(mfs, mf_hosts):
    """
    If one filter matches something that doesn't exist, and there are
    multiple filters, we should still get nothing (multiple fact first)
    """
    hf = mfs.get(filters=[
        Filter(account_numbers=['1122334']),
        Filter(canonical_facts={
            'insights_uuid': '11223344-5566-7788-99AA-000000000000'
        }),
    ])
    assert isinstance(hf, list)
    assert len(hf) == 0


def test_get_fact(service):
    facts = {"ns": {"host": "test"}}
    h = Host({"insights_id": "a"}, facts=facts)
    service.create_or_update([h])
    r = service.get([Filter(facts=facts)])
    assert len(r) == 1


def test_get_tag(service):
    tags = {"ns": {"env": "prod"}}
    h = Host({"insights_id": "a"}, tags=tags)
    service.create_or_update([h])
    r = service.get([Filter(tags=tags)])
    assert len(r) == 1


def test_index_apply_filter_to_no_hosts(index):
    hosts = set()
    hosts = index.apply_filter(Filter(), hosts)
    assert not set(hosts)
