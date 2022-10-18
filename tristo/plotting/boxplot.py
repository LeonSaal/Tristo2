import matplotlib.pyplot as plt


def boxplot_param(vals, bg, outliers, param_name, unit, limit):
    plt.boxplot(vals)
    plt.boxplot(bg, positions=[2])
    if limit:
        plt.ylim(0, 1.2 * limit)
    else:
        if unit == "µg/l":
            plt.ylim(0, 5)
    xlims = plt.xlim()
    plt.hlines([limit], *xlims, colors=["red"], lw=1)
    plt.xticks([1, 2], labels=[f"> BG,  N = {len(vals)}", f"< BG: N={len(bg)}"])
    plt.ylabel(f"{unit}")
    plt.title(param_name)
    plt.show()
    total_len = len(vals) + len(bg) + len(outliers)
    if total_len == 0:
        total_len = 1
    print(
        "; ".join(
            [
                f"{name}: N={len(x)} ({len(x)/total_len:.2%})"
                for name, x in zip(
                    ["> BG", "< BG", "not validated"], [vals, bg, outliers]
                )
            ]
        )
    )
