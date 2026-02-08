# Project Context

## Overview
We're making a tool that will automatically take google takeout folders and initially produce a csv of filenames, find exact duplicates, and propose a location for them.

## Goals & Objectives
1. Scan through a bunch of google takeout folders in a directory.
2. Create a list of files, whether they are duplicates or not, and a proposed place on disk to extract them, and output, and open this list.
3. Allow configuration of where to extract these files.
4. Fix the metadata of the files.
5. Extract the files to the proposed location on disk.
6. Cleanup the downloaded zip file as we should now have a synced folder set.
7. Allow me to paste a new set of download links from takeout, automatically download 1 at a time and integrate them into the synced library.

NEVER DELETE ANYTHING except the original zip file.

## Target Audience
Me

## Key Features

## Tech Stack
### Frontend
Tui, lets make it fancy, but still a tui

### Backend
Python or C# not sure

### Database
Just cache to local files

### Other Tools & Services
<!-- Third-party services, deployment platforms, etc. -->

## Architecture
<!-- High-level architecture overview -->

## Data Model
<!-- Key entities and relationships -->

## User Flow
<!-- How users interact with the app -->

## Development Phases
### Phase 1
Scan the folders, and create the csv.

### Phase 2
Actually enabling the 

## Open Questions
<!-- Things that need to be decided -->
-
-

## Notes
<!-- Any additional context, constraints, or considerations -->
