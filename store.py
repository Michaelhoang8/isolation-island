"""A durable, bounded MVCC state machine with snapshot and conservative OCC modes."""
import copy
import hashlib
import json
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path


class Conflict(ValueError): pass
class Missing(ValueError): pass
def canonical(x):return json.dumps(x,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False)
def digest(x):return hashlib.sha256(canonical(x).encode()).hexdigest()
def name(x):
    if not isinstance(x,str) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]{0,47}',x):raise ValueError('Names must start with an ASCII letter and use at most 48 letters, digits, underscores, or hyphens.')
    return x
def integer(x):
    if type(x) is not int or not -1000000<=x<=1000000:raise ValueError('Record values must be integers from -1,000,000 to 1,000,000.')
    return x
def seed(body):
    if not isinstance(body,dict) or set(body)!={'id','mode','records'}:raise ValueError('Provide id, mode, and records.')
    name(body['id'])
    if body['mode'] not in ('snapshot','serializable'):raise ValueError('Choose snapshot or serializable validation.')
    if not isinstance(body['records'],dict) or not 1<=len(body['records'])<=12:raise ValueError('Provide 1–12 named records.')
    return {'id':body['id'],'mode':body['mode'],'records':{name(k):integer(v) for k,v in sorted(body['records'].items())}}
def normalize(command):
    if not isinstance(command,dict):raise ValueError('Command must be an object.')
    action=command.get('action');fields={'action','tx'}
    if action=='begin':fields.add('client')
    if action in ('read','write'):fields.add('key')
    if action=='write':fields.add('value')
    if action not in ('begin','read','write','commit','abort') or set(command)!=fields:raise ValueError('Unknown action or unexpected command fields.')
    out=dict(command);name(out['tx'])
    if 'client' in out:name(out['client'])
    if 'key' in out:name(out['key'])
    if 'value' in out:integer(out['value'])
    return out
def initial(data):return {'mode':data['mode'],'head':0,'versions':{k:[{'version':0,'value':v}] for k,v in data['records'].items()},'transactions':{}}
def visible(state,key,version):return next(x for x in reversed(state['versions'][key]) if x['version']<=version)


def transition(before,command):
    c=normalize(command);s=copy.deepcopy(before);action=c['action'];tid=c['tx'];transactions=s['transactions']
    if action=='begin':
        if tid in transactions:
            if transactions[tid]['client']!=c['client']:raise Conflict('Transaction id belongs to another client.')
            return s,{'transaction':copy.deepcopy(transactions[tid]),'replayed':True}
        if len(transactions)>=100:raise Conflict('At most 100 transactions per workspace.')
        if sum(t['status']=='active' for t in transactions.values())>=12:raise Conflict('At most 12 active transactions.')
        t={'id':tid,'client':c['client'],'snapshot':s['head'],'status':'active','reads':{},'writes':{},'commit':None,'conflicts':[],'reason':None};transactions[tid]=t
        return s,{'transaction':copy.deepcopy(t),'replayed':False}
    if tid not in transactions:raise Missing('Transaction not found.')
    t=transactions[tid]
    if action in ('commit','abort') and t['status']!='active':return s,{'transaction':copy.deepcopy(t),'replayed':True}
    if t['status']!='active':raise Conflict('The transaction is terminal; start a new one.')
    if action in ('read','write'):
        key=c['key']
        if key not in s['versions']:raise Missing('Record not found. This workspace has a fixed key set.')
        if action=='write':t['writes'][key]=c['value'];return s,{'key':key,'value':c['value'],'source':'staged','transaction':copy.deepcopy(t)}
        if key in t['writes']:return s,{'key':key,'value':t['writes'][key],'version':None,'source':'staged','transaction':copy.deepcopy(t)}
        record=visible(s,key,t['snapshot']);t['reads'][key]=record['version']
        return s,{'key':key,'value':record['value'],'version':record['version'],'source':'snapshot','transaction':copy.deepcopy(t)}
    if action=='abort':t.update(status='aborted',reason='user_abort');return s,{'transaction':copy.deepcopy(t),'replayed':False}
    conflicts=[]
    for key in sorted(t['writes']):
        latest=s['versions'][key][-1]['version']
        if latest>t['snapshot']:conflicts.append({'key':key,'type':'write_write','observed':visible(s,key,t['snapshot'])['version'],'latest':latest})
    if s['mode']=='serializable':
        for key,version in sorted(t['reads'].items()):
            latest=s['versions'][key][-1]['version']
            if latest!=version and not any(x['key']==key for x in conflicts):conflicts.append({'key':key,'type':'read_changed','observed':version,'latest':latest})
    if conflicts:t.update(status='aborted',reason='validation_failed',conflicts=conflicts)
    else:
        if t['writes']:
            s['head']+=1
            for key,value in sorted(t['writes'].items()):s['versions'][key].append({'version':s['head'],'value':value})
        t.update(status='committed',commit=s['head'],reason='committed')
    return s,{'transaction':copy.deepcopy(t),'replayed':False}


class Store:
    def __init__(self,path):
        self.path=Path(path);self.path.parent.mkdir(parents=True,exist_ok=True)
        with self.db() as c:c.executescript('''PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS workspaces(id TEXT PRIMARY KEY,seed TEXT NOT NULL,state TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS events(workspace TEXT NOT NULL REFERENCES workspaces(id),sequence INTEGER NOT NULL,payload TEXT NOT NULL,hash TEXT NOT NULL,PRIMARY KEY(workspace,sequence));
        CREATE TRIGGER IF NOT EXISTS immutable_events_update BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT,'immutable event'); END;
        CREATE TRIGGER IF NOT EXISTS immutable_events_delete BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT,'immutable event'); END;
        CREATE TRIGGER IF NOT EXISTS immutable_seed BEFORE UPDATE OF id,seed ON workspaces BEGIN SELECT RAISE(ABORT,'immutable seed'); END;''')
    @contextmanager
    def db(self):
        c=sqlite3.connect(self.path,timeout=10,isolation_level=None);c.row_factory=sqlite3.Row;c.execute('PRAGMA foreign_keys=ON')
        try:yield c
        finally:c.close()
    def _row(self,c,key):
        row=c.execute('SELECT * FROM workspaces WHERE id=?',(key,)).fetchone()
        if row is None:raise Missing('Workspace not found.')
        return row
    def _audit(self,c,row):
        try:data=seed(json.loads(row['seed']));state=initial(data)
        except (ValueError,TypeError,KeyError) as e:raise Conflict('Workspace seed is corrupt.') from e
        previous=digest(data);events=[]
        for sequence,r in enumerate(c.execute('SELECT * FROM events WHERE workspace=? ORDER BY sequence',(row['id'],)),1):
            try:
                p=json.loads(r['payload']);next_state,result=transition(state,p['command'])
                if not (p['sequence']==r['sequence']==sequence and p['workspace']==row['id'] and p['previous_hash']==previous and p['before_hash']==digest(state) and p['after_hash']==digest(next_state) and p['result']==result and digest(p)==r['hash']):raise Conflict('Event verification failed.')
            except (ValueError,TypeError,KeyError) as e:raise Conflict('Stored event cannot be replayed.') from e
            state=next_state;previous=r['hash'];events.append({'event':p,'hash':r['hash'],'canonical_payload':r['payload']})
        if canonical(state)!=row['state']:raise Conflict('Stored state differs from replay.')
        return data,state,events,previous
    def create(self,body):
        data=seed(body)
        with self.db() as c:
            c.execute('BEGIN IMMEDIATE');row=c.execute('SELECT * FROM workspaces WHERE id=?',(data['id'],)).fetchone()
            if row:
                if row['seed']!=canonical(data):raise Conflict('Workspace already exists with different initial settings.')
                c.execute('COMMIT');return {'id':data['id'],'replayed':True}
            if c.execute('SELECT COUNT(*) FROM workspaces').fetchone()[0]>=20:raise Conflict('At most 20 workspaces per database.')
            c.execute('INSERT INTO workspaces VALUES(?,?,?)',(data['id'],canonical(data),canonical(initial(data))));c.execute('COMMIT');return {'id':data['id'],'replayed':False}
    def list(self):
        with self.db() as c:return [{'id':r['id'],'mode':json.loads(r['seed'])['mode']} for r in c.execute('SELECT id,seed FROM workspaces ORDER BY id')]
    def snapshot(self,key):
        name(key)
        with self.db() as c:
            c.execute('BEGIN');row=self._row(c,key);data,state,events,_=self._audit(c,row);c.execute('COMMIT');return {'seed':data,'state':state,'events':events,'verified':True}
    def command(self,key,body):
        name(key);command=normalize(body)
        with self.db() as c:
            c.execute('BEGIN IMMEDIATE');row=self._row(c,key);_,state,events,previous=self._audit(c,row)
            if len(events)>=500:raise Conflict('At most 500 command events per workspace.')
            after,result=transition(state,command);event={'format':'isolation-island-event-v1','workspace':key,'sequence':len(events)+1,'command':command,'result':result,'before_hash':digest(state),'after_hash':digest(after),'previous_hash':previous};raw=canonical(event);sha=digest(event)
            c.execute('UPDATE workspaces SET state=? WHERE id=?',(canonical(after),key));c.execute('INSERT INTO events VALUES(?,?,?,?)',(key,len(events)+1,raw,sha));c.execute('COMMIT');return {'event':event,'hash':sha,'canonical_payload':raw}
