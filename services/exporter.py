import io
from typing import List, Dict, Any
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

HEADER_FILL=PatternFill("solid",start_color="1F3864")
HEADER_FONT=Font(name="Arial",bold=True,color="FFFFFF",size=10)
BODY_FONT=Font(name="Arial",size=10)
WRAP_ALIGN=Alignment(wrap_text=True,vertical="top")
TOP_ALIGN=Alignment(vertical="top")
MAX=32000

def safe(v,m=MAX):s=str(v or "");return s[:m]+"...[cut]" if len(s)>m else s

def _head(ws,hdr):
  ws.append(hdr)
  for i,_ in enumerate(hdr,1):a=ws.cell(ws.max_row,i);a.fill=HEADER_FILL;a.font=HEADERE_FONT;a.alignment=Alignment(horizontal="center",vertical="center",wrap_text=True)
  ws.row_dimensions[ws.max_row].height=28
ww.freeze_panes=ws.cell(ws.max_row+1,1)

def _w(ws,ov=None):
  for col in ws.columns:cl=get_column_letter(col[0].column);w=min(max(max((len(str(c.value or "")) for c in col),default=0)+2,10),60);ws.column_dimensions[cl].width=ov.get(cl,w) if ov else w

def build_excel(evals,msgs):
  wb=openpyxl.Workbook()
  ws=wb.active;ws.title="Tickets & QA"
  _head(ws,["Ticket ID","Subject","Agent","Group","CSAT","Created","Resolved","Score","Complexity","Churn","Churn Reason","Contact","Summary","Coaching","Clarity","Tone","Empathy","Accuracy","Resolution","Efficiency","Ownership","Commercial"])
  for e in evals:
    s=e.get("scores") or {}
    ws.append([e.get("ticket_id"),safe(e.get("subject"),300),safe(e.get("agent_name"),100),safe(e.get("group_name"),100),e.get("csat"),str(e.get("created_at") or "")[:10],str(e.get("resolved_at") or "")[:10],e.get("total_score"),e.get("complexity"),"YES" if e.get("churn_risk_flag") else "",safe(e.get("churn_risk_reason"),200),"YES" if e.get("contact_problem_flag") else "",safe(e.get("summary"),500),safe(e.get("coaching_tip"),300),(s.get("clarity_structure") or s.get("clarity") or {}).get("score",""),(w.get("tone_professionalism") or s.get("tone") or {}).get("score",""),(s.get("empathy") or {}).get("score",""),(s.get("accuracy") or {}).get("score",""),(s.get("resolution_quality") or {}).get("score",""),(s.get("efficiency") or {}).get("score",""),(s.get("ownership") or {}).get("score",""),(s.get("commercial_awareness") or {}).get("score","")])
    for c in range(1,23):x(:ws.cell(ws.max_row,c).font=BODY_FONT
  _w(ws,{"B":50,"K":40,"M":60,"N":50})
  ws2=wb.create_sheet("Conversations");_head(ws2,["Ticket","Subject","Agent","Churn","#","Role","Time","Message"])
  meta={e["ticket_id"]:e for e in evals};cnt={}
  for m in msgs:
    t=m["ticket_id"];cnt[t]=cnt.get(t,0)+1;ev=meta.get(t,{})
    ws2.append([t,safe(ev.get("subject"),200),safe(ev.get("agent_name"),100),"YES" if ev.get("churn_risk_flag") else "",cnt[t],m["role"],str(m.get("ts") or "")[:16].replace("T"," "),safe(m["body"])])
    for c in range(1,9):ws2.cell(ws2.max_row,c).font=BODY_FONT
  _w(ws2,{"B":45,"H":120})
  buf=io.BytesIO();wb.save(buf);buf.seek(0);return buf.read()
