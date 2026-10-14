# DECISIONS.md

1. **Indexes.** I made unique indexes on `emp_code` (employees) and on `(emp_code, date)` (attendance), so the database blocks duplicates. I also made indexes on `date`, on `status` and on `department` to keep lists and reports fast. One more index on `(emp_code, punch_in)` helps punch-out find the open record. I did not make an index on `late_minutes` because the date filter already keeps the search small.

2. **Punch-in race.** Two requests try to save the same record at the same time. The unique index lets only one save. The other one fails with a duplicate error, and I send back 409.

3. **Ties.** I give ranks first, then keep the rows with rank up to the limit. So if two people are tied at the last place, both are shown.

4. **Headcount.** The summary starts from the employees list and then joins their logs. An employee with no logs is still in the list, so they are counted.

5. **100x data.** I would store a small daily summary for each department and update it on every punch. Then reports read the small summary and not millions of raw logs.
