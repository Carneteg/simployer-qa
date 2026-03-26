import asyncio
import json
import logging
import re
from typing import Dict, List
import anthropic
from config import settings

logger = logging.getLogger("simployer.claude")
_client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
CATEGORIES = ["clarity_structure","tone_professionalism","empathy","accuracy","resolution_quality","efficiency","ownership","commercial_awareness"]
EMPTY_SCORES={c:{"score":3,"reason":""} for c in CATEGORIES}
SYSTEM_PROMPT="You are a senior QA analyst for Simployer HR SaaS. Evaluate AGENT performance only. Return ONLY valid JSON. No markdown. Always close every brace."

def _build_prompt(t,thread):
  a=(t.get("responder") or {}).get("name","?")
  g=(t.get("group") or {}).get("name","?")
  tr="\n".join(f"{m['role'][0]}: {m['body'][:200]}" for m in thread[:6])
  return f"Ticket:t['id']} Agent:{A} Group:{g}\n\nCONVERSATION:\n{tr}\n\nScore 1-5 each category. total_score=avg*20. Return {{'ticket_id':'{'id']},'agent':' {a}','group':' {g}','arr':null,'complexity':'Medium','sentiment':{{'start':'Neutral','end':'Neutral'}},'scores':{json.dumps(EMPTY_SCORES)},'total_score':60,'summary':'','strengths':[''],'improvements':[''],'churn_risk_flag':false,'churn_risk_reason':null,'contact_problem_flag':false,'coaching_tip':''}}"

def _repair(t):
  t=re.sub(r"^```json\s*","",t);t=re.sub(r"^```\s*","",t);t=re.sub(r"```\s*$","",t);t=t.strip()
  if not t.endswith("}"):
    d=t.count("{")-t.count("}")
    if 0<d<=7:t=(t+'"') if t.count('"')%2 else t;t+="}"*d
  return t

def _norm(ev):
  s=ev.setdefault("scores",{})
  for c in CATEGORIES:s.setdefault(c,{"score":3,"reason":""})
  v=[s[c].get("score",3) for c in CATEGORIES]
  ev["total_score"]=round(sum(v)/len(v)*20,1)
  s["clarity"]=s.get("clarity_structure");s["tone"]=s.get("tone_professionalism")
  for k in ["summary","strengths","improvements","churn_risk_flag","churn_risk_reason","contact_problem_flag","coaching_tip","arr","complexity"]:ev.setdefault(k,[] if k in("strengths","improvements") else False if "flag" in k else None if k in("churn_risk_reason","arr") else "")
  return ev

async def eval_ticket(tick,thread,retries=5):
  p=_build_prompt(tick,thread)
  for i in range(retries):
    try:
      r=await _client.messages.create(model="claude-haiku-4-5-20251001",max_tokens=1200,temperature=0,system=SYSTEM_PROMPT,messages=[{"role":"user","content":p}])
      return _norm(json.loads(_repair(r.content[0].text)))
    except anthropic.RateLimitError:await asyncio.sleep(2**(i+3))
    except json.JSONDecodeError:
      if i==retries-1:raise
      await asyncio.sleep(3)
    except:
      if i==retries-1:raise
      await asyncio.sleep(2)
  raise RuntimeError("max retries")
