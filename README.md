# brukerbridge

Converts raw imaging data into more useful formats and handles I/O between the Bruker computer and oak.

Please address your thank you notes for having to maintain/use this software to the engineers at Bruker who did not release a linux executable for their utility that converts their custom format to tiffs.

## Usage

### Initial setup

All users must create a config json file in brukerbridge/users. It is VERY important that the source code on the brukerbridge be properly version controlled, follow this protocol precisely to add or update config:

1. verify that there is no unchecked in work in the brukerbridge copy of the source code
   - on the brukerbridge computer open a new command prompt
   - execute `cd src\brukerbridge`
   - execute `git status`
   - if this says `up to date with origin/master` and there is are no unstaged or untracked files move on to the next step
   - if there are unstaged or untracked files, they must be either removed or committed to the repo. do this very carefully, this could break things. if you don't know how to do this contact whoever the current boffin is
2. make a local clone of the brukerbridge source code
3. add your config file (see next section)
4. commit it and push to master
5. on the brukerbridge computer, execute `git pull`

#### config format and imaging folder naming requirements

You must specify your config as a json file which shares a name with your imaging folder on the Bruker computer (case-sensitive). For instance if you name your config `levitsky.json` then your imaging folder on Bruker must be named `levitsky`. The contents of `levitsky` would be something like `levitsky/[current date]/TSeries-[current date]`. Using dates to label imaging sessions is just a convention, you can name them anything you want. You could do `levitsky/go_bears/beat_stanford` and as long as `beat_stanford` contains a valid Bruker xml file brukerbridge will happily process your images.

Within the json the following fields are mandatory:

- `oak_target`: string. absolute path to where you want your imagery to end up on oak. must start with `X:` which is the drive oak is mapped to on the brukerbridge computer
-  `convert_to`: string. `nii.gz`, `nii`. `nii.gz` is favored for storage savings.

Optional fields:
- `max_image_size`: int. Desired maximum image size of converted images in bytes. Acquisitions which exceed this size limit will be split into chunks of less than `max_image_size` bytes. Under rare circumstance chunks might be larger than this limit, but only by about 500 bytes. `bridge` no longer needs to buffer acquisitions in memory, so the only reason to use this setting if you are doing very large recordings and expect that your downstream processing won't fit into memory on sherlock. 
- `add_to_build_que`: bool. legacy. it's a brainsss thing.


Deprecated fields:

- `transfer_fictrac`: bool. deprecated. doesn't do anything. omit this and move your fictrac files manually.
- `email`: you will not receive any emails. just check the logs.
- `split`: no longer has any effect. use `max_image_size`


Here's a minimal example:

```json
{
    "oak_target": "X:/data/levitsky/imaging/import",
    "convert_to": "nii.gz"
}
```


### Once you've finished an imaging session

From the Bruker computer, open the `brukerbridge.bat` icon in the desktop, select the imaging session you want to process. This is typically named as the current date and must contain Tseries. If you get a failed connection error it's probably because the server process isn't running. Walk over to D217 and start it then try again.

## Troubleshooting

There are two processes that run in tandem, a server process that just handles IO between the Bruker computer and the Brukerbridge computer and a `bridge` process which does everything else.

### Troubleshooting the server process

- check the logs. `C:\Users\User\Desktop\dataflow_logs`
- make sure the server is running

author: Bella Brezovec

maintainer: Andrew Berger

### Troubleshooting the bridge process

1. check the logs! they are usually quite informative. use `baretail` which highlights more severe messages in bright colors. logs live in `C:\Users\User\logs`. logs from previous days are renamed appropriately. `bridge.log` has messages at level INFO and above. `bridge_error.log` contains only ERROR and CRITICAL messages. `bridge_debug.log` contains everything.
2. if bridge crashed due to a recoverable error (such as OOM), restart using the `launch bridge` icon on the desktop.
3. if all else fails, contact Andrew Berger (andbberger@gmail.com)

author: Andrew Berger

maintainer: Andrew Berger


## How do I.......

### Change the target drive on the Brukerbridge computer?

scripts/launch_bridge.bat

### update python/windows/move to a new computer?

don't.

### Install a larger hard drive in the Brukerbridge computer?

you can't. the old drives were replaced in July 2024 with the best possible configuration for the motherboard (4 4TB SSDs in RAID 10). the `bridge` process has high random I/O requirements, performance will tank on hard drives.

## Things you might want to know

- `bridge` runs ripping, conversion and oak I/O in parallel for every person and acquisition. If you've been waiting a long time for your files it's probably because it's still running the conversion step.
- `bridge` looks for (acquisition) directories containing a valid Bruker xml file within a (session) directory suffixed by `__queue__`. the queue suffix is removed once all acquisitions from a session have been processed.
- an empty file named `.complete` is added to an acquistion directory once it has been processed to mark it as complete
- files are deleted from the Bruker computer once the transfer is validated, but they are NOT deleted from the brukerbridge once processing is copmlete. you must clean out your processed files, and do so frequently because the hard drive is not that big. the reason for this is that is difficult to validate checksums on oak and that this would be an expensive feature to develop.
- each channel is written as its own NIfTI. NIfTI supports multi-channel images, but `bridge`'s streaming io logic doesn't. might be possible if NIfTI's internal axis order is a certain way. exercise to the reader (very hard).
- bidirectional z scans are not supported. they used to be, and they could again but it's not trivial. refer to the source of `brukerbridge.io` for an explanation. exercise to the reader (hard)
- single-plane imaging is supported
- single images are not supported. Just use the tiffs the ripper generates. Or do a single-plane t-series with one time point.
- there are no longer any constraints to the size of NIfTIs `bridge` can write. however, once you get over 64GB or so they start to get really unwieldly to work with on sherlock (our largest nodes have 256GB of memory), so you may want to set the `max_image_size` field in the config to limit size.
- if `bridge` encounters an irrecoverable error processing an acquisition that acquisition will be suffixed by `__error__` andignored. This often happens when an acquisition contains an invalid xml. Check the logs to find out what happened, if the error is recoverable manually simply delete error suffix and `bridge` will try again. You may have to add the `__queue__` suffix to the parent session directory of all other acquisitions from that session have already been processed.
  
  
## Notes to future maintainers

### Test infrastructure

There is a fairly comprehensive set of tests for io and PV5.8 conversion logic. It was designed in such a way to facilitate straightforward expansion to other PV versions. Platform specific tests can be marked as such by decorating them with `pytest.mark.win32`,  `pytest.mark.darwin` or `pytest.mark.linux`. 

You will need `git lfs` to clone the test data. While the raw PV5.8 test data was checked in as is, only a tarball is provided for the ripped PV5.8 data as there many thousands of files.  I had to break up into chunks to stay under Github's file size limit. Reassemble with `cat ripped_test_acq.tar.gz.part.* > ripped_test_acq.tar.gz`. Extract it in situ and rename `mv 20250523_test ripped_test_acq`. Do not attempt to check in the extracted data.

## Support for previous and future PrairieView versions

As of the writing, `brukerbridge` supports only PV5.8. Adding support for PV5.8 was a substantial effort which required a complete rewrite of the conversion logic. A `pv5.5` tag was left on latest working PV5.5 commit. 

To add support for a future PV version, you would do something like this:

1. Obtain a test data set, ideally covering the same test cases as the PV5.8 test dataset. These acquistions would ideally be as small as possible, but the plane series multi-page acquisitions will have to be fairly long to have more than one page.
2. Duplicate the PV5.8 fixtures in `tests/conftest.py`
3. Duplicate the PV5.8 tests in `tests/test_common_conversion_logic.py` and `tests/test_pv58_conversion_logic.py`
4. Add a module for the new version under `brukerbridge.conversion` and add matching implementations for all methods in `brukerbridge.conversion.pv58`. Add a matching import in `brukerbridge.conversion.__init__`. It is quite possible that many if not all of the methods in `brukerbridge.conversion.pv58` would work out of the box with future versions of PrairieView, however because Bruker does not provide a spec for their XMLs, and because the data integrity for the entire lab is at stake, you should be extremely careful in examining all output files for each test case while inferring the spec. This should take at least a week. 
5. Add the ripping utility for the new PV version to the brukerbridge server
6. Update the constants in `brukerbridge.conversion.common`. I have decided to multiplex versions by having a dictionary from PV version into the corresponding conversion module (`CONVERSION_MODULES`), which assumes to some extent that conversion modules for different PV versions will have the same methods. May god have mercy on your soul if things are so different that this wasn't a good assumption.

The `contains_valid_xml` method is probably the worst (only?) offender, in terms of violating this abstraction. I kept it in bridge because I wanted to do some detailed logging that would only be apropriate there. You'll want to review it. 

You should also carefully verify the behavior of the `ripping_complete` method. It's failure to detect that voltage recordings weren't done was a major foot gun in the past.

