
The release of the binaries is fully automated. Do not create a Github release
manually.

#### Release Process
1. Create a new tag for the next available release number, see
   https://github.com/solana-labs/stake-o-matic/tags, and push it to the repo:
   eg, `git tag v42 && git push origin v42`
2. The GitHub workflow automatically triggers a new build, creates a release
   with the name of the tag, and uploads the release artifacts.  You can monitor
   the release process at https://github.com/solana-labs/stake-o-matic/actions
