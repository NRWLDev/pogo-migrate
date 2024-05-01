## New migrations

To create a new migration use `pogo new`. This will template out the migration
file and open the file in your configured text editor (`vi` by default).

Supported flags:

- `--sql` generate a sql migration (defaults to `.py`)
- `--no-interactive` skip the editor step and just write the migration template
  to the migrations directory.

```bash
$ pogo new -m "a descriptive message"
```
