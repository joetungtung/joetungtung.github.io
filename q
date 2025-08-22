jira:
  base_url: "https://your-jira.local"      # 你們 Jira 的網址（on‑prem）
  username: "jira_user"                     # 可用基本認證的帳號（或 PAT 使用者）
  password: "jira_password_or_pat"          # 密碼或個人存取權杖
  verify_cert: "C:/path/to/ca-bundle.pem"   # 企業憑證，若內網自簽；不需要可設 true
  project_key: "TPPRD"                      # 你們專案的 key（用來 auto-create）
  issuetype: "Incident"                      # 預設開單型別
  labels: ["auto", "mailbot"]               # 預設加上的 labels
  search_lookback_days: 30                  # 比對視窗（例如比對近 30 天內是否已開過）
  dryrun_create: true                       # true=只查不開；確認 OK 再改成 false








import requests
from datetime import datetime, timedelta

# ---------- JIRA client ----------
class JiraClient:
    def __init__(self, cfg):
        j = cfg["jira"]
        self.base = j["base_url"].rstrip("/")
        self.auth = (j.get("username"), j.get("password")) if j.get("username") else None
        self.verify = j.get("verify_cert", True)
        self.project_key = j.get("project_key")
        self.issuetype = j.get("issuetype", "Task")
        self.labels = j.get("labels", [])
        self.lookback_days = int(j.get("search_lookback_days", 30))
        self.dryrun_create = bool(j.get("dryrun_create", True))
        self.session = requests.Session()
        if self.auth:
            self.session.auth = self.auth
        self.session.verify = self.verify
        self.session.headers.update({"Accept": "application/json", "Content-Type": "application/json"})

    def _url(self, path):
        return f"{self.base}{path}"

    def get_issue(self, key):
        r = self.session.get(self._url(f"/rest/api/2/issue/{key}"))
        if r.status_code == 200:
            return r.json()
        return None

    def search(self, jql, fields=("summary","status","created","priority","issuetype","reporter")):
        data = {
            "jql": jql,
            "startAt": 0,
            "maxResults": 50,
            "fields": list(fields)
        }
        r = self.session.post(self._url("/rest/api/2/search"), json=data)
        r.raise_for_status()
        return r.json().get("issues", [])

    def create_issue(self, summary, description, priority=None, assignee=None, extra_fields=None):
        if self.dryrun_create:
            print(f"[jira] DRYRUN would create: {summary}")
            return None
        fields = {
            "project": {"key": self.project_key},
            "summary": summary,
            "description": description,
            "issuetype": {"name": self.issuetype},
            "labels": self.labels,
        }
        if priority:
            fields["priority"] = {"name": priority}
        if assignee:
            fields["assignee"] = {"name": assignee}  # 若你們是雲端需用 accountId；on‑prem 多半用 name
        if extra_fields:
            fields.update(extra_fields)

        r = self.session.post(self._url("/rest/api/2/issue"), json={"fields": fields})
        r.raise_for_status()
        return r.json()  # 包含 key









def normalize_subject(subj: str) -> str:
    s = (subj or "").strip()
    # 把常見日期/時間/流水號移除，避免比對受干擾（依你們告警格式可再加規則）
    s = re.sub(r"\b\d{4}/\d{1,2}/\d{1,2}\b", "", s)     # 2025/08/22
    s = re.sub(r"\b\d{1,2}:\d{2}(:\d{2})?\b", "", s)    # 09:30 或 09:30:15
    s = re.sub(r"\b\d{8,}\b", "", s)                    # 連號
    s = re.sub(r"\s+", " ", s).strip()
    return s

def make_jql_from_email(cfg, subj: str, body: str) -> str:
    """
    基本策略：以標題為主、輔以 body 關鍵詞，限制在某專案、某段期間內。
    """
    j = cfg["jira"]
    project = j["project_key"]
    lookback_days = int(j.get("search_lookback_days", 30))
    since = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")  # Jira JQL 用系統時區即可
    ns = normalize_subject(subj)

    # 用 quotes 包起來避免被 JQL 解析成多詞
    terms = []
    if ns:
        terms.append(f'text ~ "\\"{ns}\\""')
    # 可選：從 body 擷取一兩個穩定關鍵字（例如 Dynatrace 主機或 component 名）
    host = match_one(r"host[:=]\s*([A-Za-z0-9._-]+)", body)
    if host:
        terms.append(f'text ~ "\\"{host}\\""')

    base = f'project = {project} AND created >= "{since}"'
    if terms:
        base += " AND " + " AND ".join(terms)

    # 你也可以加 issuetype 限制、排除已關閉等
    # base += ' AND statusCategory != Done'
    return base









# ---- 沒被略過：先依規則判斷（result["action"] 會是 LINK_BY_KEY 或 CREATE_OR_SEARCH）----
result = decide(cfg, subj, body)

issue_key = result.get("issue_key", "").strip()
final_action = result["action"]
jira_link = ""

if issue_key:
    # 直接用 Key 查
    issue = jira.get_issue(issue_key)
    if issue:
        jira_link = f"{jira.base}/browse/{issue_key}"
        final_action = "LINK_BY_KEY"
    else:
        # Key 看起來像，但 Jira 查不到 → 改走全文搜尋
        issue_key = ""
        final_action = "CREATE_OR_SEARCH"

if not issue_key:
    # 用 JQL 搜索相似 issue
    jql = make_jql_from_email(cfg, subj, body)
    try:
        candidates = jira.search(jql)
    except Exception as e:
        candidates = []
        print("[jira] search failed:", e)

    if candidates:
        hit = candidates[0]
        issue_key = hit["key"]
        jira_link = f"{jira.base}/browse/{issue_key}"
        final_action = "LINK_BY_SEARCH"
    else:
        # 沒找到 → （乾跑 或 真開單）
        summary = normalize_subject(subj)[:120] or subj[:120]
        description = f"Auto-created from mail.\n\nSubject: {subj}\n\nBody:\n{body[:5000]}"
        created = jira.create_issue(summary=summary, description=description,
                                    priority=result.get("priority") or None)
        if created and "key" in created:
            issue_key = created["key"]
            jira_link = f"{jira.base}/browse/{issue_key}"
            final_action = "CREATED"
        else:
            # dryrun 或建立失敗
            final_action = "CREATE_DRYRUN" if jira.dryrun_create else "CREATE_FAILED"

# 最終列印
print(pad(dt,20), pad(frm,28), pad(subj,64),
      pad(final_action,16), pad(issue_key,12),
      pad(result.get("priority",""),10), jira_link or "非skip")
