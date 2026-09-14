#!/bin/bash

npm install -g @lightdash/cli@0.2864.4
lightdash login http://localhost:8080 --token ldpat_a3e442d410e698da7cdb28b15ea3d72d
lightdash deploy --create
lightdash deploy --f nextgen_bi --project-name nextgen_bi --profiles-dir nextgen_bi
lightdash deploy 