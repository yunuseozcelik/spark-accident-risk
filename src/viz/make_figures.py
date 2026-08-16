"""Faz 6 — outputs/metrics içindeki sonuçlardan statik grafikler üretir.

Sadece pandas/matplotlib kullanır (Spark gerekmez); `outputs/metrics/`
altındaki JSON/CSV dosyalarını okuyup `outputs/figures/` altına PNG yazar.
Bir girdi dosyası yoksa ilgili grafik atlanır (aşamalar bağımsız çalışabilir).

Kullanım:
    .venv/Scripts/python.exe src/viz/make_figures.py
"""
import json
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.use("Agg")

METRICS = Path("outputs/metrics")
FIGURES = Path("outputs/figures")
FIGURES.mkdir(parents=True, exist_ok=True)

# --- Palette (bkz. dataviz skill referans paleti) ---
INK_PRIMARY = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
BLUE = "#2a78d6"
ORANGE = "#eb6834"
AQUA = "#1baf7a"
BLUE_RAMP = ["#9ec5f4", "#5598e7", "#256abf", "#0d366b"]  # step 200/350/500/700

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.size": 10,
    "text.color": INK_PRIMARY,
    "axes.edgecolor": AXIS,
    "axes.labelcolor": INK_SECONDARY,
    "xtick.color": INK_SECONDARY,
    "ytick.color": INK_SECONDARY,
    "axes.grid": True,
    "grid.color": GRID,
    "grid.linewidth": 0.8,
    "figure.facecolor": "#fcfcfb",
    "axes.facecolor": "#fcfcfb",
    "savefig.facecolor": "#fcfcfb",
})


def _clean_axes(ax):
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    ax.spines["left"].set_color(AXIS)
    ax.spines["bottom"].set_color(AXIS)
    ax.grid(axis="y", zorder=0)
    ax.set_axisbelow(True)


def severity_distribution():
    p = METRICS / "initial_eda.json"
    if not p.exists():
        return
    dist = json.loads(p.read_text())["severity_distribution"]
    sev = sorted(dist, key=int)
    counts = [dist[s] for s in sev]
    total = sum(counts)

    fig, ax = plt.subplots(figsize=(6, 4))
    bars = ax.bar(sev, counts, color=BLUE_RAMP, width=0.6, zorder=3)
    for b, c in zip(bars, counts):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height(), f"{c/total:.1%}",
                 ha="center", va="bottom", fontsize=9, color=INK_SECONDARY)
    ax.set_xlabel("Severity")
    ax.set_ylabel("Kaza sayısı")
    ax.set_title("Severity dağılımı (n=7.728.394)", color=INK_PRIMARY, fontsize=11, loc="left")
    _clean_axes(ax)
    fig.tight_layout()
    fig.savefig(FIGURES / "severity_distribution.png", dpi=150)
    plt.close(fig)


def hourly_high_risk_rate():
    p = METRICS / "agg_by_hour.csv"
    if not p.exists():
        return
    df = pd.read_csv(p).sort_values("hour")

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(df["hour"], df["high_risk_rate"], color=BLUE, linewidth=2, zorder=3)
    ax.scatter(df["hour"], df["high_risk_rate"], color=BLUE, s=18, zorder=4)
    rush = df[df["hour"].isin([7, 8, 9, 16, 17, 18])]
    ax.scatter(rush["hour"], rush["high_risk_rate"], color=ORANGE, s=32, zorder=5,
               label="Rush hour (07-09, 16-18)")
    ax.set_xlabel("Saat")
    ax.set_ylabel("Yüksek-şiddet oranı (Severity ≥ 3)")
    ax.set_title("Saate göre yüksek-şiddet oranı", color=INK_PRIMARY, fontsize=11, loc="left")
    ax.set_xticks(range(0, 24, 2))
    ax.legend(frameon=False, loc="lower left")
    _clean_axes(ax)
    fig.tight_layout()
    fig.savefig(FIGURES / "hourly_high_risk_rate.png", dpi=150)
    plt.close(fig)


def model_comparison():
    p = METRICS / "model_comparison.csv"
    if not p.exists():
        return
    df = pd.read_csv(p)

    for task, metrics_wanted, fname, title in [
        ("severity", ["accuracy", "weighted_f1"], "model_comparison_severity.png",
         "Çok sınıflı Severity — model karşılaştırması"),
        ("high_risk", ["weighted_f1", "roc_auc", "high_risk_recall"], "model_comparison_high_risk.png",
         "İkili high-risk — model karşılaştırması"),
    ]:
        sub = df[(df["task"] == task) & (df["metric"].isin(metrics_wanted))]
        if sub.empty:
            continue
        pivot = sub.pivot(index="model", columns="metric", values="value")[metrics_wanted]

        colors = [BLUE, ORANGE, AQUA][: len(metrics_wanted)]
        fig, ax = plt.subplots(figsize=(7, 4.5))
        n_models, n_metrics = pivot.shape
        x = range(n_models)
        width = 0.8 / n_metrics
        for i, (metric, color) in enumerate(zip(metrics_wanted, colors)):
            offs = [xi + (i - (n_metrics - 1) / 2) * width for xi in x]
            bars = ax.bar(offs, pivot[metric].values, width=width, color=color,
                           label=metric, zorder=3)
            for b, v in zip(bars, pivot[metric].values):
                ax.text(b.get_x() + b.get_width() / 2, b.get_height(), f"{v:.2f}",
                         ha="center", va="bottom", fontsize=8, color=INK_SECONDARY)
        ax.set_xticks(list(x))
        ax.set_xticklabels(pivot.index)
        ax.set_ylim(0, 1.08)
        ax.set_ylabel("Değer")
        ax.set_title(title, color=INK_PRIMARY, fontsize=11, loc="left")
        ax.legend(frameon=False, loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=n_metrics)
        _clean_axes(ax)
        fig.tight_layout()
        fig.savefig(FIGURES / fname, dpi=150)
        plt.close(fig)


def confusion_matrix_best():
    p = METRICS / "confusion_matrices.json"
    if not p.exists():
        return
    cms = json.loads(p.read_text())
    binary = cms.get("high_risk", {})
    model = "gbt" if "gbt" in binary else next(iter(binary), None)
    if model is None:
        return
    cm = binary[model]
    labels = sorted(cm.keys(), key=int)
    mat = [[cm.get(r, {}).get(c, 0) for c in labels] for r in labels]
    mat_norm = [[v / sum(row) if sum(row) else 0 for v in row] for row in mat]

    fig, ax = plt.subplots(figsize=(4.5, 4))
    im = ax.imshow(mat_norm, cmap="Blues", vmin=0, vmax=1)
    ax.set_xticks(range(len(labels)))
    ax.set_yticks(range(len(labels)))
    ax.set_xticklabels(["low-risk (0)", "high-risk (1)"] if labels == ["0", "1"] else labels)
    ax.set_yticklabels(["low-risk (0)", "high-risk (1)"] if labels == ["0", "1"] else labels)
    ax.set_xlabel("Tahmin")
    ax.set_ylabel("Gerçek")
    ax.set_title(f"Karışıklık matrisi — {model} (high_risk)", color=INK_PRIMARY, fontsize=11, loc="left")
    for i, row in enumerate(mat):
        for j, v in enumerate(row):
            frac = mat_norm[i][j]
            ax.text(j, i, f"{v:,}\n({frac:.1%})", ha="center", va="center",
                     fontsize=9, color="white" if frac > 0.5 else INK_PRIMARY)
    for spine in ax.spines.values():
        spine.set_visible(False)
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    fig.tight_layout()
    fig.savefig(FIGURES / "confusion_matrix_high_risk.png", dpi=150)
    plt.close(fig)


def feature_importance():
    p = METRICS / "feature_importances.json"
    if not p.exists():
        return
    imps = json.loads(p.read_text())
    binary = imps.get("high_risk", {})
    model = "random_forest" if "random_forest" in binary else next(iter(binary), None)
    if model is None:
        return
    items = sorted(binary[model].items(), key=lambda kv: kv[1], reverse=True)[:10]
    names = [k for k, _ in items][::-1]
    vals = [v for _, v in items][::-1]

    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.barh(names, vals, color=BLUE, zorder=3)
    ax.set_xlabel("Önem (featureImportances)")
    ax.set_title(f"Öznitelik önemi — {model} (high_risk, top 10)", color=INK_PRIMARY, fontsize=11, loc="left")
    _clean_axes(ax)
    ax.grid(axis="x", zorder=0)
    ax.grid(axis="y", visible=False)
    fig.tight_layout()
    fig.savefig(FIGURES / "feature_importance_high_risk.png", dpi=150)
    plt.close(fig)


def main() -> None:
    made = []
    for fn in [severity_distribution, hourly_high_risk_rate, model_comparison,
               confusion_matrix_best, feature_importance]:
        before = set(FIGURES.glob("*.png"))
        fn()
        after = set(FIGURES.glob("*.png"))
        made.extend(sorted(p.name for p in after - before))
    print("Yazıldı:", ", ".join(made) if made else "(hiçbir girdi bulunamadı)")


if __name__ == "__main__":
    main()
