Deliberately broken manifests. Every rule in manifests_test.go violates here at least once, so
TestTheGuardCatchesWhatItClaimsTo can prove the guard still bites. Never fix anything in this
directory: a green fixture makes the guard a test nobody has seen fail.
