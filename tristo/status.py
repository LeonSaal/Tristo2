from dataclasses import dataclass


@dataclass
class Status:
    OK: str = "OK"
    ERROR: str = "ERROR"
    OMITTED: str = "OMITTED"
    DOWNLOAD: str = "DOWNLOAD"

    YES: str = "YES"
    NO: str = "NO"
    CONFIRMED: str = "CONFIRMED"

    IMG: str = "IMG"
    SCAN: str = "SCAN"
    N_PAGES: str = "N_PAGES"
    DECOMP: str = "DECOMP_ERROR"
    DENSITY: str = "DENSITY"

    COMMERCIAL: str = "COMMERCIAL"
    PRESS: str = "PRESS"
    COMMUNITY: str = "COMMUNITY"
    PROBABLY: str = "PROBABLY"

    CONTENT: str = "CONTENT"
    CREATION_DATE: str = "CREATION_DATE"
    ENCRYPTED: str = "ENCRYPTED"
    FORM: str = "FORM"
    FNAME: str = "FNAME"
    NONE: str = "NONE"

    CAMELOT: str = "camelot"
    TABULA: str = "tabula"
    INSERTED: str = "INSERTED"

    MEDIAN: str = "MEDIAN"
    AV: str = "AVERAGE"
