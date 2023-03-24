# CMSDeletionCampaignSpring2023

Deletion campaign scripts required for tape and disk cleanup. The input of this script is a list of datasets to be deleted. The jupyter notebook [here](DeletionCampaignSpring2023.ipynb) will find ALL the rules for the specified datasets [here](https://raw.githubusercontent.com/rappoccio/Summer22TapeDeletion/main/dropping_sorted_fall2022.csv) and export this information to a csv file. The csv file will be used as input for the script which will update the rule to delete the lock and remove it purging replicas.

Basically the process for this Deletion Campaign is:

1. CMS Datasets Identification
2. Rules matching Information
   - [Deletion information](deletion_rules_spring2023.csv): Dataset, Rule_id, RSE, RSE_TYPE, FILES_NUMBER, FILES_SIZE
   - Space Usage to free [here](DeletionCampaignStats.pdf)
3. List Orphan Tape Replicas: replicas that doesn’t have rules and will be deleted because of the deletion mode
4. Deletion dataset discussion: share the deletion information with owners and wait for confirmation
5. Disk Deletion
6. Tape deletion (one RSE by the time)
   - Configure site for deletion mode. (Reaper=True, Deletion=1, Read=0, Write=0, third_party_copy=1)
