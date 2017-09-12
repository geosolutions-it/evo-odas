try:
    from settings import *
    from override.settings import *
    from override.secrets import *
    from s2_msi_l1c import *
    from override.s2_msi_l1c import *
    from s1_grd_1sdv import *
    from override.s1_grd_1sdv import *
    from s1_grd_1sdh import *
    from override.s1_grd_1sdh import *
    # legacy
    from xcom_keys import *
    from workflow_settings import *
except:
    pass