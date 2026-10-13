"""Private reader credentials stay in memory and curl stdin; no automatic query retry."""
import json, os, pathlib, re, subprocess, time, urllib.parse

def reader_connection():
    host=os.environ.get('CLICKHOUSE_HOST')
    password=os.environ.get('CLICKHOUSE_PASSWORD')
    user=os.environ.get('CLICKHOUSE_USER','default')
    if host and password:
        return host,password,user
    # Local fallback is git-ignored. No credentials are included in evidence.
    repo_root=next(p for p in pathlib.Path(__file__).resolve().parents if (p/'.git').exists())
    source=(repo_root/'full-path-realtime/quotes/clickhouse-cloud/_credentials.txt').read_text().split('READER',1)[1]
    host=re.search(r'--host\s+(\S+)',source).group(1)
    password=re.search(r"--password\s+'([^']+)'",source).group(1)
    return host,password,user

def request(sql, query_id=None, database='quotes_async_t4'):
    host,password,user=reader_connection()
    params={'readonly':'2','wait_end_of_query':'1','database':database}
    if query_id: params['query_id']=query_id
    url='https://'+host+':8443/?'+urllib.parse.urlencode(params)
    config='\n'.join(['url = '+json.dumps(url),'user = '+json.dumps(user+':'+password),
        'data-binary = '+json.dumps(sql),'silent','show-error','max-time = 40','include'])
    start=time.perf_counter()
    response=subprocess.run(['curl','--config','-'],input=config.encode(),capture_output=True)
    wall=time.perf_counter()-start
    output=response.stdout.decode().replace(password,'[redacted]')
    error=response.stderr.decode().replace(password,'[redacted]')
    # curl includes the HTTP CONNECT block, then the origin response headers.
    output=output.replace('\r\n','\n')
    blocks=output.split('\n\n')
    while len(blocks)>1 and blocks[0].startswith('HTTP/'):
        headers=blocks.pop(0)
    body='\n\n'.join(blocks)
    status=int(headers.splitlines()[0].split()[1]) if 'headers' in locals() else 0
    summary={}
    for line in headers.splitlines():
        if line.lower().startswith('x-clickhouse-summary:'):
            summary=json.loads(line.split(':',1)[1].strip())
    if response.returncode or status!=200:
        raise RuntimeError(json.dumps({'curl_exit':response.returncode,'http_status':status,'error':error,'body':body[:3000]}))
    return {'body':body,'headers':headers,'summary':summary,'client_wall_s':wall}

def bounded(sql, threads=2, output='CSVWithNames'):
    return sql.rstrip().rstrip(';')+f" SETTINGS max_threads={threads},max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=1000000000,max_bytes_to_read=100000000000,skip_unavailable_shards=0 FORMAT {output}"
