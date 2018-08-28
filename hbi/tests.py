import os
import grpc
import hbi.hbi_pb2 as p
import hbi.hbi_pb2_grpc as g

from hbi.server import Host, Filter, Service, serve
from hbi.hbi_pb2 import HostList, FilterList, CanonicalFact
from hbi.util import names
from pytest import fixture


@fixture
def service():
    return Service()


@fixture
def grpc_client():
    server = serve()
    connect_str = f"localhost:{os.environ.get('PORT', '50051')}"
    with grpc.insecure_channel(connect_str) as ch:
        yield g.HostInventoryStub(ch)
    server.stop(0)


def gen_host_list():
    return [Host({"hostname": n}, display_name=f"{n}.com")
            for n in ("-".join(dn) for dn in names())]


def test_create(service):
    host_list = gen_host_list()
    ret_hostnames = {h.canonical_facts["hostname"]
                     for h in service.create_or_update(host_list)}
    original_hostnames = {h.canonical_facts["hostname"] for h in host_list}
    assert ret_hostnames == original_hostnames


def test_grpc(grpc_client):
    host_list = HostList(hosts=[
        p.Host(
            display_name="-".join(display_name),
            canonical_facts=[
                CanonicalFact(
                    key="hostname",
                    value=f"{'-'.join(display_name)}.com",
                )
            ]) for display_name in names()
    ])

    ret = grpc_client.CreateOrUpdate(host_list, None)
    assert len(ret.hosts) == len(host_list.hosts)
    assert host_list.hosts[0].display_name == ret.hosts[0].display_name
    filters = [p.Filter(ids=[ret.hosts[0].id])]
    assert len(grpc_client.Get(FilterList(filters=filters), None).hosts) == 1


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


def test_get_all(service):
    hosts = gen_host_list()
    service.create_or_update(hosts)
    assert len(service.get()) == len(hosts)


def test_get_one(service):
    hosts = gen_host_list()
    filters = Filter(ids=[service.create_or_update(hosts)[0].id])
    assert len(service.get([filters])) == 1


def test_get_fact(service):
    h = Host({"insights_id": "a"}, facts={"host": "test"})
    service.create_or_update([h])
    r = service.get([Filter(facts={"host": "test"})])
    assert len(r) == 1


def test_get_tag(service):
    h = Host({"insights_id": "a"}, tags={"env": "prod"})
    service.create_or_update([h])
    r = service.get([Filter(tags={"env": "prod"})])
    assert len(r) == 1
