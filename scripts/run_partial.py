import brukerbridge as bridge

extensions_for_oak_transfer = ['.nii', '.csv', '.xml', 'json', '.txt'] # last 4 chars

### Directory on this computer to process ###
# full_target = 'G:/Max/20210611'
# full_target = 'G:/Tim/20210427'
# full_target = 'G:/Avery/20210614'
full_target = 'G:/Ashley/20210802'
#full_target = 'F:/Ashley/20210709_2'
#full_target = 'X:/data/Ashley2/imaging_raw/20210709'

### Oak target ###
# oak_target = 'X:/data/Brezovec/2P_Imaging/imports'
oak_target = 'X:/data/Ashley2/imports'
# oak_target = 'X:/data/Max/ImagingData/Bruker/imports'
# oak_target = 'X:/data/Tim/ImagingData/imports'
# oak_target = 'X:/data/krave/bruker_data/imports/test'

### raw to tiff ###
bridge.convert_raw_to_tiff(full_target)

### tiffs to nii or tiff stack ###
#bridge.convert_tiff_collections_to_nii(full_target)
bridge.convert_tiff_collections_to_nii(full_target)
#bridge.convert_tiff_collections_to_stack(full_target)

### Transfer to oak ###
bridge.start_oak_transfer(full_target, oak_target, extensions_for_oak_transfer, "False")