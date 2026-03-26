import asyncio
import logging
import re
from asyncio import Semaphore
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import httpx
from config import settings

logger = logging.getLogger("simployer.freshdesk")
_sem = Semaphore(5)
CHURN_KEYWORDS = ["oppsigelse","cancel","avslutte","sier opp","avslutning","switching","bytte system","vurderer andre","not satisfied","very disappointed","tredje gang","third time","säger upp","avsluta","säga upp"]

async def fd_get(url,n=5):
  async with _sem:
    for i in range(n):
      try:
        async with httpx.AsyncClient(timeout=30) as c:
          r = await c.get(url,auth=(settings.freshdesk_api_key,"X"))
          if r.status_code==429:
            await asyncio.sleep(int(r.headers.get("retry-after",60)));continue
          r.raise_for_status();return r.json()
      except:
        if i==n-1:raise
        await asyncio.sleep(2**i)
    raise RuntimeError(f"max retries: {url}")

async def fetch_all_tickets(days_back,since=None):
  if not since: since=(datetime.now(timezone.utc)-timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
  base=f"https://{settings.freshdesk_domain}/api/v2/tickets"
  tickets,page=[],1
  while True:
    batch=await fd_get(f"{base}?updated_since={since}&per_page=100&page={page}&include=stats,requester,description&order_by=updated_at&order_type=desc")
    if not batch:break
    tickets.extend([t for t in batch if t.get("status") in(4,5)])
    if len(batch)<100:break
    page+=1;await asyncio.sleep(0.2)
  return tickets

async def fetch_conversations(tid):
  try:return await fd_get(f"https://{settings.freshdesk_domain}/api/v2/tickets/{tid}/conversations") or []
  except:return []

def strip_html(h):
  if not h:return ""
  h = re.sub(r"<script[\s\S]*?</script>","",h,flags=re.I)
  h = re.sub(r"<style[\s\S]*?</style>","",h)
  h = re.sub(r"<br \s/?>","\n",h)
  h = re.sub(r"<[^>]+>","",h)
  h = h.replace("&amp;","&").replace("&lt;","<").replace("&gt;",">").replace("&nbsp;"," ")
  return re.sub(r"\n{3,}","\n\n",h).strip()

def build_thread(ticket,convs):
  thread=[]
  d=strip_html(ticket.get("description") or ticket.get("description_text") or "")
  if d and len(d)>5:thread.append({"role":"CUSTOMER","ts":ticket.get("created_at",""),"body":d})
  for c in sorted(convs,key=lambda x:x.get("created_at","")):
    body=strip_html(c.get("body",""))
    if not body or len(body)<5 or c.get("private"):continue
    thread.append({"role":"AGENT" if c.get("incoming") is False else "CUSTOMER","ts":c.get("created_at",""),"body":body})
  return thread

def detect_churn(thread):
  t=" ".join(m["body"] for m in thread if m["role"]=="CUSTOMER").lower()
  return next((kw for kw in CHURN_KEYWORDS if kw in t),None)

def frt_minutes(ticket):
  try:
    f=(ticket.get("stats") or {}).get("first_responded_at")
    if not f:return None
    return int((datetime.fromisoformat(f.replace("Z","+00:00"))-datetime.fromisoformat(ticket["created_at"].replace("Z","+00:00"))).total_seconds()/60)
  except:return None
