import asyncio
import json
import logging
from datetime import datetime, timezone
from sqlalchemy import select, delete
from sqlalchemy.dialects.postgresql import insert as pg_insert
from database import AsyncSessionLocal
from models import Run, Ticket, Message, Evaluation
from services.freshdesk import fetch_all_tickets, fetch_conversations, build_thread, detect_churn, frt_minutes
from services.claude import eval_ticket

logger = logging.getLogger("simployer.evaluator")

async def evaluate_run(ctx,run_id):
  redis=ctx["redis"]
  async with AsyncSessionLocal() as db:
    run=await db.get(Run,run_id)
    if not run:return
    user_id=run.user_id;run.status="running";run.started_at=datetime.now(timezone.utc);await db.commit()
    async def push(d,t,c):await redis.publish(f"run:{run_id}",json.dumps({"run_id":run_id,"done":d,"total":t,"churn":c,"pct":round(d/t*100) if t else 0,"status":"running"}))
    try:
      tickets=await fetch_all_tickets(run.days_back);run.tickets_total=len(tickets);await db.commit()
      for i,ticket in enumerate(tickets):
        tid=str(ticket["id"])
        try:
          convs=await fetch_conversations(tid);thread=build_thread(ticket,convs);churn_kw=detect_churn(thread)
          stmt=pg_insert(Ticket).values(id=tid,user_id=user_id,subject=ticket.get("subject"),agent_name=(ticket.get("responder") or {}).get("name"),group_name=(ticket.get("group") or {}).get("name"),status=ticket.get("status"),priority=ticket.get("priority"),csat=(ticket.get("satisfaction_rating") or {}).get("rating"),tags=ticket.get("tags") or [],fr_escalated=bool(ticket.get("fr_escalated")),nr_escalated=bool(ticket.get("nr_escalated")),created_at=ticket.get("created_at"),resolved_at=(ticket.get("stats") or {}).get("resolved_at"),updated_at=ticket.get("updated_at")).on_conflict_do_update(index_elements=["id","user_id"],set_={"subject":ticket.get("subject"),"agent_name":(ticket.get("responder") or {}).get("name"),"updated_at":ticket.get("updated_at")})
          await db.execute(stmt)
          await db.execute(delete(Message).where(Message.ticket_id==tid,Message.user_id==user_id))
          for m in thread:db.add(Message(ticket_id=tid,user_id=user_id,role=m["role"],ts=m["ts"] or None,body=m["body"]))
          await db.commit();await asyncio.sleep(0.3)
          ev=await eval_ticket(ticket,thread)
          await db.execute(delete(Evaluation).where(Evaluation.ticket_id==tid,Evaluation.user_id==user_id,Evaluation.run_id==run_id))
          db.add(Evaluation(ticket_id=tid,user_id=user_id,run_id=run_id,total_score=ev.get("total_score"),complexity=ev.get("complexity"),sentiment_start=(ev.get("sentiment") or {}).get("start"),sentiment_end=(ev.get("sentiment") or {}).get("end"),summary=ev.get("summary"),churn_risk_flag=ev.get("churn_risk_flag",False),churn_risk_reason=ev.get("churn_risk_reason") or (f'Signal: "{churn_kw}"' if churn_kw else None),contact_problem_flag=ev.get("contact_problem_flag",False),coaching_tip=ev.get("coaching_tip"),strengths=ev.get("strengths"),improvements=ev.get("improvements"),scores=ev.get("scores")))
          run.tickets_done=i+1;(run.churn_count)=+1 if ev
+("churn_risk_flag") or churn_kw else None;await db.commit();await push(i+1,len(tickets),run.churn_count)
        except Exception as e:logger.error(f"FAILED #{tid}: {e}");run.tickets_done=i+1;await db.commit();continue
        await asyncio.sleep(2)
      run.status="done";run.finished_at=datetime.now(timezone.utc);await db.commit()
      await redis.publish(f"run:{l¶Qid}",json.dumps({"run_id":run_id,"status":"done","done":run.tickets_done,"total":run.tickets_total,"churn":run.churn_count,"pct":100}))
    except Exception as e:
      run.status="failed";run.error=str(e);run.finished_at=datetime.now(timezone.utc);await db.commit()
      await redis.publish(f"run:{l¶Qid}",json.dumps({"run_id":run_id,"status":"failed","error":str(e)}));raise
