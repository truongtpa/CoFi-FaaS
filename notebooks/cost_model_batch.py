import os
import re
import sys
import csv
import glob
import json
import argparse
import statistics
import math

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cost_model as cm

VARIANTS = {"s3": "bloomfaas-s3", "nfs": "bloomfaas-nfs"}
BASE = "starling-base"


def runs(qdir, tag):
    return sorted(d for d in glob.glob(os.path.join(qdir, f"*--{tag}*")) if os.path.exists(os.path.join(d, "cost-stages.jsonl")))


def med(xs):
    xs = [x for x in xs if x is not None and not (isinstance(x, float) and math.isnan(x))]
    return statistics.median(xs) if xs else None


def summarize(q, variant, reports):
    b = [r["runs"]["bf"]["totals"] for r in reports]
    n = [r["runs"]["nobf"]["totals"] for r in reports]
    d = [r["decision"] for r in reports]
    ap = [a for r in reports for j in r["bf"] for a in j["appliers"]]
    phi = [abs(a["err_phi_%"]) for a in ap if a.get("err_phi_%") is not None]
    dout = [abs(a["err_D_out_%"]) for a in ap if a.get("err_D_out_%") is not None]
    no_sj = sorted({a["stage"] for a in ap if not a.get("join_for_sigma")})
    rok = [j["R_model"] == j["R_meas"] for r in reports for j in r["bf"]]
    return {
        "query": q, "variant": variant, "runs": len(reports),
        "T1_meas": med(x["T_meas"] for x in b), "T1_model": med(x["T_model"] for x in b), "err_T1_%": med(x["err_T_%"] for x in b),
        "T0_meas": med(x["T_meas"] for x in n), "T0_model": med(x["T_model"] for x in n), "err_T0_%": med(x["err_T_%"] for x in n),
        "C1_meas": med(x["C_meas"] for x in b), "C1_model": med(x["C_model"] for x in b), "err_C1_%": med(x["err_C_%"] for x in b),
        "C0_meas": med(x["C_meas"] for x in n), "C0_model": med(x["C_model"] for x in n), "err_C0_%": med(x["err_C_%"] for x in n),
        "speedup_meas": med(x["T0_meas"] / x["T1_meas"] for x in d),
        "saving_meas": med(1 - x["C1_meas"] / x["C0_meas"] for x in d),
        "bf_better_meas": sum(x["bf_better_meas"] for x in d), "bf_better_model": sum(x["bf_better_model"] for x in d),
        "decision_match": sum(x["decision_match"] for x in d),
        "n_bf": len(reports[0]["bf"]), "R_ok": f"{sum(rok)}/{len(rok)}",
        "med_abs_err_phi_%": med(phi), "med_abs_err_Dout_%": med(dout),
        "no_semijoin": " ".join(no_sj),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("history", nargs="?", default="history")
    ap.add_argument("--out", default="notebooks/cost-report/tpch100-v5")
    ap.add_argument("--params")
    ap.add_argument("--calib", choices=["loqo", "nobf"], default="loqo",
                    help="loqo: kappa from all runs of the other queries; nobf: kappa from the paired no-BF run")
    ap.add_argument("--per-instance", action="store_true", help="d_v = max_i d_{v,i} from each function's own D_{v,i}; C^C = sum_i d_{v,i}")
    ap.add_argument("--merge-work", action="store_true", help="add W^{bf-merge} = p^b_j m_j to the merge_bf stage")
    ap.add_argument("--waves", action="store_true", help="d_v = makespan of p_v functions on max_parallel runner slots")
    ap.add_argument("--serial", action="store_true", help="Runner executes stages one by one: T = sum of stage times")
    ap.add_argument("--rel-fit", action="store_true", help="calibrate compute rates on relative error")
    ap.add_argument("--disp-median", action="store_true", help="kappa^disp = median dispatch instead of mean")
    ap.add_argument("--contention", action="store_true", help="storage bandwidth shared by concurrent functions")
    ap.add_argument("--orch", action="store_true", help="add the calibrated runner gap kappa^orch to every stage")
    a = ap.parse_args()
    P = cm.load_params(a.params)
    P["per_instance"] = P["per_instance"] or a.per_instance
    P["merge_work"] = P["merge_work"] or a.merge_work
    P["waves"] = P["waves"] or a.waves
    P["orch"] = P["orch"] or a.orch
    P["serial"] = P["serial"] or a.serial
    P["rel_fit"] = P["rel_fit"] or a.rel_fit
    P["disp_median"] = P["disp_median"] or a.disp_median
    P["contention"] = P["contention"] or a.contention

    qdirs = sorted((d for d in glob.glob(os.path.join(a.history, "*")) if os.path.isdir(d)),
                   key=lambda d: int(re.sub(r"\D", "", os.path.basename(d)) or 0))
    data = {}
    for qdir in qdirs:
        q = os.path.basename(qdir).lower()
        data[q] = {v: [cm.load(d, P) for d in runs(qdir, t)] for v, t in {**VARIANTS, "base": BASE}.items()}
        print(f"loaded {q}")

    rows, kappas = [], {}
    for q, R in data.items():
        if a.calib == "loqo":
            K = cm.calibrate([r for q2, R2 in data.items() if q2 != q for rs in R2.values() for r in rs], P)
            kappas[q] = {k: K[k] for k in ("bw", "lat", "disp", "rate", "qlat", "r_or", "orch", "beta", "rk")}
        for variant in VARIANTS:
            reports = []
            for i, (bf, nobf) in enumerate(zip(R[variant], R["base"]), 1):
                if a.calib == "nobf":
                    K = cm.calibrate([nobf], P)
                    fb = cm.calibrate([bf, nobf], P)
                    K["rate"], K["qlat"] = {**fb["rate"], **K["rate"]}, {**fb["qlat"], **K["qlat"]}
                reports.append(cm.analyze(bf, nobf, K, P, os.path.join(a.out, q, f"{variant}-run{i}"), a.calib, quiet=True))
            if reports:
                rows.append(summarize(q, variant, reports))
                print(f"{q:4} {variant:3} runs={len(reports)}")

    with open(os.path.join(a.out, "summary.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    json.dump({"calib": a.calib, "rows": rows, "kappa": kappas}, open(os.path.join(a.out, "summary.json"), "w"), indent=2, default=str)
    print(f"saved -> {a.out}/summary.csv ({len(rows)} rows)")


if __name__ == "__main__":
    main()
