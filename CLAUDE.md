# CLAUDE.md

## Language

- Converse with the user in Spanish.
- Everything written to the repository (docs, logs, code, comments) is in English.

## Prompt log

On every user prompt, before doing anything else, append an entry to `docs/PROMPTS.md`:

- Heading: `## YYYY-MM-DD HH:MM:SS (UTC-03:00)` — the user is in UTC-3 and the system clock reports UTC, so get the time with:
  `TZ='<-03>3' date '+%Y-%m-%d %H:%M:%S (UTC%:z)'`
- Body: the prompt text translated faithfully to English (not summarized).
