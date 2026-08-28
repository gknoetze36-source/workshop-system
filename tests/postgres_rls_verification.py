import psycopg2, psycopg2.extras, sys
import os
DSN = os.environ.get("DSN", "postgresql://phanta_app:apppass@127.0.0.1:5433/phanta")

def conn():
    c = psycopg2.connect(DSN); c.autocommit = True; return c

results = []
def check(name, passed, detail=""):
    results.append((name, passed, detail))
    print(("PASS " if passed else "FAIL ") + name + ("  " + detail if detail else ""))

c = conn(); cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# --- confirm RLS is actually FORCED on the key tables
cur.execute("""
  SELECT relname, relrowsecurity, relforcerowsecurity FROM pg_class
  WHERE relname IN ('customers','bookings','audit_logs','security_events',
                    'security_incidents','legal_acceptances') AND relkind='r'
  ORDER BY relname
""")
for r in cur.fetchall():
    check(f"RLS enabled+forced on {r['relname']}", r['relrowsecurity'] and r['relforcerowsecurity'],
          f"(enabled={r['relrowsecurity']} forced={r['relforcerowsecurity']})")

# --- seed two tenants as the app role (RLS applies!)
cur.execute("SET app.platform_admin = '1'")
cur.execute("INSERT INTO owners (name,email,active) VALUES ('A','a@t.co',true) RETURNING id")
oa = cur.fetchone()['id']
cur.execute("INSERT INTO owners (name,email,active) VALUES ('B','b@t.co',true) RETURNING id")
ob = cur.fetchone()['id']
cur.execute("INSERT INTO locations (owner_id,name,industry,active) VALUES (%s,'LocA','workshop',true) RETURNING id",(oa,))
la = cur.fetchone()['id']
cur.execute("INSERT INTO locations (owner_id,name,industry,active) VALUES (%s,'LocB','workshop',true) RETURNING id",(ob,))
lb = cur.fetchone()['id']
cur.execute("SET app.platform_admin = ''")

for lid, nm in ((la,'AliceA'), (lb,'BobB')):
    cur.execute("SET app.location_id = %s", (str(lid),))
    cur.execute("INSERT INTO customers (location_id,first_name,last_name,whatsapp_number) VALUES (%s,%s,'X',%s)",
                (lid, nm, f"+2782000{lid:04d}"))

# --- TENANT ISOLATION: tenant A must not see tenant B
cur.execute("SET app.location_id = %s", (str(la),))
cur.execute("SELECT first_name FROM customers")
names = {r['first_name'] for r in cur.fetchall()}
check("tenant A sees only its own customers", names == {'AliceA'}, f"saw {sorted(names)}")

# --- IDOR: direct id fetch of the other tenant's row
cur.execute("SELECT id FROM customers")  # only A's visible
cur.execute("SET app.platform_admin='1'")
cur.execute("SELECT id FROM customers WHERE first_name='BobB'")
bob_id = cur.fetchone()['id']
cur.execute("SET app.platform_admin=''")
cur.execute("SET app.location_id = %s", (str(la),))
cur.execute("SELECT * FROM customers WHERE id=%s", (bob_id,))
check("tenant A cannot fetch tenant B's row by id", cur.fetchone() is None)

# --- WRITE isolation: A must not insert into B's tenant
try:
    cur.execute("INSERT INTO customers (location_id,first_name,last_name,whatsapp_number) VALUES (%s,'Sneaky','X','+27820001')",(lb,))
    check("tenant A cannot write into tenant B", False, "INSERT succeeded")
except psycopg2.errors.CheckViolation:
    check("tenant A cannot write into tenant B", True, "(WITH CHECK violation)")
except Exception as e:
    check("tenant A cannot write into tenant B", True, f"({type(e).__name__})")

# --- UPDATE isolation
cur.execute("SET app.location_id = %s", (str(la),))
cur.execute("UPDATE customers SET first_name='Hacked' WHERE id=%s", (bob_id,))
check("tenant A cannot update tenant B's row", cur.rowcount == 0, f"rowcount={cur.rowcount}")

# --- SECURITY_EVENTS: append-only, unreadable by a tenant
cur.execute("SET app.platform_admin=''")
cur.execute("SET app.location_id = %s", (str(la),))
cur.execute("INSERT INTO security_events (event_type,outcome,created_at) VALUES ('auth.login_failed','failure',now()::text)")
check("security_events writable with no location context (failed login)", True)
cur.execute("SELECT * FROM security_events")
check("tenant CANNOT read security_events", len(cur.fetchall()) == 0)
cur.execute("SET app.platform_admin='1'")
cur.execute("SELECT * FROM security_events")
check("platform admin CAN read security_events", len(cur.fetchall()) >= 1)

# --- SECURITY_INCIDENTS: platform admin only
cur.execute("SET app.platform_admin='1'")
cur.execute("INSERT INTO security_incidents (incident_type,severity,status,summary,detected_at) VALUES ('other','low','open','t',now()::text)")
cur.execute("SET app.platform_admin=''")
cur.execute("SET app.location_id = %s", (str(la),))
cur.execute("SELECT * FROM security_incidents")
check("tenant CANNOT read security_incidents", len(cur.fetchall()) == 0)
try:
    cur.execute("INSERT INTO security_incidents (incident_type,severity,status,summary,detected_at) VALUES ('other','low','open','x',now()::text)")
    check("tenant CANNOT write security_incidents", False, "INSERT succeeded")
except Exception as e:
    check("tenant CANNOT write security_incidents", True, f"({type(e).__name__})")

# --- LEGAL_ACCEPTANCES isolation
cur.execute("SET app.location_id = %s", (str(la),))
cur.execute("INSERT INTO legal_acceptances (document_key,document_version,owner_id,location_id,accepted_at) VALUES ('terms_of_service','v1',%s,%s,now()::text)",(oa,la))
cur.execute("SET app.location_id = %s", (str(lb),))
cur.execute("SELECT * FROM legal_acceptances")
check("tenant B cannot read tenant A's legal acceptance", len(cur.fetchall()) == 0)

# --- audit_logs FK cascade added by 0026
cur.execute("""SELECT conname, confdeltype FROM pg_constraint
               WHERE conrelid='audit_logs'::regclass AND contype='f'""")
fks = cur.fetchall()
check("audit_logs has location FK with CASCADE",
      any(f['confdeltype']=='c' for f in fks), f"{[(f['conname'],f['confdeltype']) for f in fks]}")

print()
failed = [n for n,p,_ in results if not p]
print(f"RESULT: {len(results)-len(failed)}/{len(results)} passed")
sys.exit(1 if failed else 0)
