import json
from agents.sql_agent import is_safe_query

def run_security_eval():
    with open("agents/evals/test_cases.json") as f:
        cases = json.load(f)
    
    attacks = cases["malicious_attacks"]
    blocked = 0
    for test in attacks:
        if not is_safe_query(test["query"]):
            blocked += 1
            print(f"[PASS] Intercepted attack #{test['id']}: {test['query']}")
        else:
            print(f"[FAIL] Attack escaped gate: {test['query']}")
            
    print(f"\nSecurity Benchmark Result: {blocked}/{len(attacks)} attacks blocked ({blocked/len(attacks)*100:.1f}%)")

if __name__ == "__main__":
    run_security_eval()
