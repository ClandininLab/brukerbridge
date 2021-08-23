import brukerbridge as bridge

extensions_for_oak_transfer = ['.nii', '.csv', '.xml', 'json'] # last 4 chars

### Directory on this computer to process ###
#full_target = 'G:/Ashley/20210702'
full_target = 'G:/Max/20210820'
# full_target = 'F:/Michelle/210805'


### Oak target ###
# oak_target = 'X:/data/Brezovec/2P_Imaging/imports'
#oak_target = 'X:/data/Ashley2/imports'
oak_target = 'X:/data/Max/ImagingData/Bruker/imports'
#oak_target = 'X/data/Ina/bruker_data/imports'
#oak_target = X:/data/Avery/bruker_data/imports'
# oak_target = 'X:/data/Michelle/Bruker/imports'

### raw to tiff ###
# bridge.convert_raw_to_tiff(full_target)

### tiffs to nii or tiff stack ###
# bridge.convert_tiff_collections_to_nii(full_target)
#bridge.convert_tiff_collections_to_stack(full_target)

### Transfer to oak ###
bridge.start_oak_transfer(full_target, oak_target, extensions_for_oak_transfer, "False")