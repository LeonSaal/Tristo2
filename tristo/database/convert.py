import logging

import numpy as np
from pint import (DefinitionSyntaxError, DimensionalityError,
                  OffsetUnitCalculusError, UndefinedUnitError, UnitRegistry)

from ..config import LOG_FMT
from ..utils import is_number

logging.basicConfig(level=logging.INFO, format=LOG_FMT, style="{")
logger = logging.getLogger(__name__)

ureg = UnitRegistry()



def convert_val(val, from_unit:str, to_unit:str):
    if not is_number(val) or (val == None):
        return np.nan
    try:
        converted_val = (
            ureg.Quantity(f"{val} {from_unit}").to(to_unit).magnitude
        )
    except (DimensionalityError, UndefinedUnitError, ValueError, OffsetUnitCalculusError, AttributeError,DefinitionSyntaxError) as e:
        #logger.error(f'{e}')
        try:
            converted_val = float(val)
        except:
            converted_val = np.nan
    return converted_val
