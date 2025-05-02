####################################################################
#
# This file is part of exfor-parser.
# Copyright (C) 2022 International Atomic Energy Agency (IAEA)
#
# Disclaimer: The code is still under developments and not ready
#             to use. It has been made public to share the progress
#             among collaborators.
# Contact:    nds.contact-point@iaea.org
#
####################################################################
import re

# from .config import session_lib
from .config import session
from .models import Endf_Reactions

# reactions = Endf_Reactions.query.filter_by(type='residual').first()
reactions = session.query(Endf_Reactions).filter(Endf_Reactions.type =='residual').all()
# reactions = session_lib().query(Endf_Reactions).filter(Endf_Reactions.type =='residual').limit(100000).all()
# reactions = session_lib().query(Endf_Reactions).filter(Endf_Reactions.type =='residual').first()

# i = 0
for r in reactions:
    nuclides = re.split(r'(\d+)', r.residual)
    if len(nuclides[1]) == 2:
        # i += 1
        print( f"{nuclides[0]}{nuclides[1].zfill(3)}{nuclides[2]}" )
        r.residual = f"{nuclides[0]}{nuclides[1].zfill(3)}{nuclides[2]}"

    # if i > 5:
    #     break
    
    # break

# session_lib.flush()
session.commit()
