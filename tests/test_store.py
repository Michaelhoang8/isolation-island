import concurrent.futures
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from store import Store,Conflict,Missing


class MvccTests(unittest.TestCase):
    def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'mvcc.db';self.s=Store(self.path)
    def tearDown(self):self.tmp.cleanup()
    def create(self,mode='snapshot',key='lab'):return self.s.create({'id':key,'mode':mode,'records':{'east':1,'west':1,'counter':0}})
    def cmd(self,action,tx='A',key='lab',**kw):return self.s.command(key,{'action':action,'tx':tx,**kw})['event']['result']
    def skew(self,mode):
        self.create(mode)
        for tx in ('A','B'):
            self.cmd('begin',tx,client=tx)
            for record in ('east','west'):self.assertEqual(self.s.command('lab',{'action':'read','tx':tx,'key':record})['event']['result']['value'],1)
        self.s.command('lab',{'action':'write','tx':'A','key':'east','value':0});self.s.command('lab',{'action':'write','tx':'B','key':'west','value':0})
        return self.cmd('commit','A'),self.cmd('commit','B'),self.s.snapshot('lab')['state']
    def test_snapshot_allows_write_skew_but_occ_rejects_changed_read(self):
        a,b,state=self.skew('snapshot');self.assertEqual(a['transaction']['status'],'committed');self.assertEqual(b['transaction']['status'],'committed');self.assertEqual(state['versions']['east'][-1]['value']+state['versions']['west'][-1]['value'],0)
        self.s=Store(Path(self.tmp.name)/'second.db');a,b,state=self.skew('serializable');self.assertEqual(b['transaction']['status'],'aborted');self.assertEqual(b['transaction']['conflicts'],[{'key':'east','type':'read_changed','observed':0,'latest':1}]);self.assertEqual(state['versions']['west'][-1]['value'],1)
    def test_repeatable_snapshot_and_read_own_writes_survive_restart(self):
        self.create();self.cmd('begin',client='reader');self.cmd('begin','B',client='writer')
        self.s.command('lab',{'action':'write','tx':'B','key':'counter','value':5});self.cmd('commit','B');self.s=Store(self.path)
        self.assertEqual(self.s.command('lab',{'action':'read','tx':'A','key':'counter'})['event']['result']['value'],0)
        self.s.command('lab',{'action':'write','tx':'A','key':'counter','value':9});own=self.s.command('lab',{'action':'read','tx':'A','key':'counter'})['event']['result'];self.assertEqual((own['value'],own['source']),(9,'staged'));self.assertEqual(self.cmd('commit')['transaction']['status'],'aborted')
    def test_concurrent_writers_one_commit_under_both_modes(self):
        for mode in ('snapshot','serializable'):
            self.create(mode,mode)
            for i in range(10):self.s.command(mode,{'action':'begin','tx':f'T{i}','client':f'C{i}'});self.s.command(mode,{'action':'write','tx':f'T{i}','key':'counter','value':i+1})
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:results=list(pool.map(lambda i:self.s.command(mode,{'action':'commit','tx':f'T{i}'})['event']['result']['transaction']['status'],range(10)))
            self.assertEqual(results.count('committed'),1);self.assertEqual(self.s.snapshot(mode)['state']['head'],1)
    def test_multi_record_commit_is_atomic_and_terminal_retries_add_no_versions(self):
        self.create();self.cmd('begin',client='A')
        for k in ('east','west'):self.s.command('lab',{'action':'write','tx':'A','key':k,'value':7})
        first=self.cmd('commit');again=self.cmd('commit');self.assertTrue(again['replayed']);self.assertEqual(first['transaction'],again['transaction']);state=self.s.snapshot('lab')['state'];self.assertEqual(state['head'],1);self.assertEqual([state['versions'][k][-1]['version'] for k in ('east','west')],[1,1])
        with self.assertRaises(Conflict):self.s.command('lab',{'action':'write','tx':'A','key':'east','value':1})
    def test_serializable_read_only_validation_and_aba_version_detection(self):
        self.create('serializable');self.cmd('begin',client='A');self.s.command('lab',{'action':'read','tx':'A','key':'counter'})
        for tx,v in [('B',5),('C',0)]:self.cmd('begin',tx,client=tx);self.s.command('lab',{'action':'write','tx':tx,'key':'counter','value':v});self.cmd('commit',tx)
        result=self.cmd('commit')['transaction'];self.assertEqual(result['status'],'aborted');self.assertEqual(result['conflicts'][0]['latest'],2)
    def test_begin_replay_abort_and_read_only_commit_do_not_advance_head(self):
        self.create();self.cmd('begin',client='A');self.assertTrue(self.cmd('begin',client='A')['replayed']);self.cmd('abort');self.assertTrue(self.cmd('commit')['replayed']);self.cmd('begin','B',client='B');self.cmd('commit','B');self.assertEqual(self.s.snapshot('lab')['state']['head'],0)
        with self.assertRaises(Conflict):self.cmd('begin',client='different')
    def test_sql_failure_rolls_back_projection_and_event(self):
        self.create();self.cmd('begin',client='A');self.s.command('lab',{'action':'write','tx':'A','key':'east','value':0});before=self.s.snapshot('lab')
        with self.s.db() as c:c.execute("CREATE TRIGGER fail BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT,'injected'); END")
        with self.assertRaises(sqlite3.IntegrityError):self.cmd('commit')
        self.assertEqual(self.s.snapshot('lab'),before)
    def test_tampering_immutable_history_and_input_validation(self):
        self.create();self.cmd('begin',client='A')
        with self.s.db() as c:
            with self.assertRaises(sqlite3.IntegrityError):c.execute('DELETE FROM events')
            with self.assertRaises(sqlite3.IntegrityError):c.execute("UPDATE workspaces SET seed='{}'")
            c.execute("UPDATE workspaces SET state='{}'")
        with self.assertRaises(Conflict):self.s.snapshot('lab')
        for v in [True,1.5,1000001]:
            with self.assertRaises(ValueError):self.s.create({'id':'bad','mode':'snapshot','records':{'x':v}})
    def test_unknown_keys_cannot_create_phantom_records(self):
        self.create();self.cmd('begin',client='A')
        with self.assertRaises(Missing):self.s.command('lab',{'action':'read','tx':'A','key':'missing'})
        with self.assertRaises(Missing):self.s.command('lab',{'action':'write','tx':'A','key':'missing','value':0})
