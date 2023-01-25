import matplotlib.pyplot as plt
from pint import UnitRegistry
from sqlalchemy import select
from sqlalchemy.orm import Session

from tristo.database import Param

from ..database import get_vals
from ..paths import PATH_PLOTS
from .violinplot import violinplot


def violin_most_common(session:Session):
    param_ids = [11,46,32,43,10,26,35,31,42,30]
    fig, axs =plt.subplots(1,len(param_ids))
    for param_id, ax in zip(param_ids, axs):
        vals = get_vals(param_id,session=session)
        legend = violinplot(*vals, ax=ax)
    fig.legend(*legend, loc='lower center', ncol=2, bbox_to_anchor=(0.5,-0.1))
    plt.tight_layout(w_pad=0.3)
    fig.text(-.01,0.4,'Concentration in mg/l', rotation='vertical')
    fig.savefig(PATH_PLOTS / 'dist_frequent_param.png')

def violin_common_org(session:Session):
    ureg=UnitRegistry()
    to_unit = 'µg/l'
    param_ids = [55, 4,79,73,94,28,27,16,13]
    fig, axs =plt.subplots(1,len(param_ids))
    for param_id, ax in zip(param_ids, axs):
        alias = session.execute(select(Param.alias).where(Param.id==param_id)).scalar_one()
        param_name, limit, unit, df = get_vals(param_id, session=session)
        factor = ureg.Quantity(unit).to(to_unit).magnitude
        df.val = df.val*factor
        limit = limit*factor
        if alias:
            param_name=alias
        legend = violinplot(param_name, limit, unit, df, ax=ax)
    fig.legend(*legend, loc='lower center', ncol=2, bbox_to_anchor=(0.5,-0.1))
    plt.tight_layout(w_pad=0.3)
    fig.text(-.01,0.5,f'Concentration in {to_unit}', rotation='vertical')
    fig.savefig(PATH_PLOTS / 'dist_frequent_param_2.png')
