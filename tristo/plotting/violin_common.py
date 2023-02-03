import matplotlib.pyplot as plt
from pint import UnitRegistry
from sqlalchemy import select
from sqlalchemy.orm import Session

from tristo.database import Param

from ..database import get_vals
from ..paths import PATH_PLOTS
from .violinplot import violinplot


def violin_most_common(session: Session):
    param_ids = [11, 41, 46, 10, 32, 35, 43, 26, 42, 31]
    fig, axs = plt.subplots(1, len(param_ids))
    for param_id, ax in zip(param_ids, axs):
        vals = get_vals(param_id, session=session)
        legend = violinplot(*vals, ax=ax, legend = 'Limit [1]')
    fig.legend(*legend, loc="lower center", ncol=2, bbox_to_anchor=(0.5, -0.1))
    plt.tight_layout(w_pad=0.3)
    fig.text(-0.01, 0.4, "Concentration in mg/L", rotation="vertical")
    fig.savefig(PATH_PLOTS / "dist_frequent_param")


def violin_common_org(session: Session):
    ureg = UnitRegistry()
    to_unit = "µg/L"
    param_ids = [55, 4, 94, 28, 79, 73, 99, 16, 27, 13]
    fig, axs = plt.subplots(1, len(param_ids))
    for param_id, ax in zip(param_ids, axs):
        param_name, limit, unit, df = get_vals(param_id, session=session)
        factor = ureg.Quantity(unit).to(to_unit).magnitude
        df.val = df.val * factor
        limit = limit * factor
        legend = violinplot(param_name, limit, unit, df, ax=ax, legend= 'Respective $^1$Limit [1] or $^2$guidance value [26]')
    fig.legend(*legend, loc="lower center", ncol=2, bbox_to_anchor=(0.5, -0.1))
    plt.tight_layout(w_pad=0.3)
    fig.text(-0.01, 0.5, f"Concentration in {to_unit}", rotation="vertical")
    fig.savefig(PATH_PLOTS / "dist_frequent_param_2")
