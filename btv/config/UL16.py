import order as od

campaign = od.Campaign("UL16", 0, ecm=13, bx=25)


base_config = cfg = od.Config(campaign)

cfg.set_aux(
    "working_points",
    {
        "deepjet": {
            "loose": 0.0508,
            "medium": 0.2598,
            "tight": 0.6502,
        },
        "deepcsv": {
            "loose": 0.2027,
            "medium": 0.6001,
            "tight": 0.8819,
        },
    },
)


dataset_tt_dl = od.Dataset(
    "tt_dl",
    1,
    campaign=campaign,
    n_files=317,
    keys=[
        "/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/mullerd-RunIISummer20UL16PFNanoAODAPV-106X_mcRun2_asymptotic_preVFP_v8-v2-177b19191fd6c7257e9667203a87cdd8/USER",
    ],
    aux={"instance": "prod/phys03"},
)

cfg.add_dataset(dataset_tt_dl)
