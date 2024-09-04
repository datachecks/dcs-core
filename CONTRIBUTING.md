# Contributing to DCS Core

Thank you for considering contributing to Datachecks!

## How can you contribute?
We welcome both code and non-code contributions. You can:
* Report a bug
* Improve documentation
* Submit a bug fix
* Propose a new feature or improvement
* Contribute a new feature or improvement
* Test DataChecks

## Code contributions
Here is the general workflow:
* Fork the Datachecks repository
* Clone the repository
* Make the changes and commit them
* Push the branch to your local fork
* Make sure that all the tests are passing successfully
* Submit a Pull Request with described changes
* We follow exactly one commit in the pull request. This will help us to have a clean git history

### Useful git commands for fork based development

#### Creating a Fork
```bash
git clone git@github.com:USERNAME/datachecks.git
```

#### Keeping Your Fork Up to Date
```bash
# Add 'upstream' repo to list of remotes
git remote add upstream https://github.com/datachecks/dcs-core

# Verify the new remote named 'upstream'
git remote -v

# Fetch from upstream remote
git fetch upstream

# View all branches, including those from upstream
git branch -va
```

#### Create a Branch
```bash
# Checkout the main branch - you want your new branch to come from master
git checkout main

# Create a new branch named newfeature (give your branch its own simple informative name)
git branch newfeature

# Switch to your new branch
git checkout newfeature
```

#### Cleaning Up Your Work
Prior to submitting your pull request, you might want to do a few things to clean up your branch and make it as simple as possible for the original repo's maintainer to test, accept, and merge your work.

If any commits have been made to the upstream master branch, you should rebase your development branch so that merging it will be a simple fast-forward that won't require any conflict resolution work.
```bash
# Fetch upstream master and merge with your repo's master branch
git fetch upstream
git checkout main
git merge upstream/main

# If there were any new commits, rebase your development branch
git checkout newfeature
git rebase main

```
Now, it may be desirable to squash some of your smaller commits down into a small number of larger more cohesive commits. You can do this with an interactive rebase:

```bash
# Rebase all commits on your development branch
git checkout
git rebase -i main
```


### Additional information
- Datachecks is under active development.
- We are happy to receive a Pull Request for bug fixes or new functions for any section of the platform. If you need help or guidance, you can open an Issue first.
- Because it is in the process of significant refactoring! If you want to contribute, please first come to our [Slack channel](https://join.slack.com/t/datachecks/shared_invite/zt-1zqsigy4i-s5aadIh2mjhdpVWU0PstPg) for a quick chat.
- We highly recommend that you open an issue, describe your contribution, share all needed information there and link it to a Pull Request.
- We evaluate Pull Requests taking into account: code architecture and quality, code style, comments & docstrings and coverage by tests.