# Birthday Reminder App 

If you’re reading this, congratulations — you care more about my birthday app than I expected.

This is a tiny Azure Functions app that:
- pulls birthday info from SQL,
- sends daily and monthly summary emails,
- and tries very hard not to forget people (unlike me when rugby is on).

## Why this exists

I’m a data engineer. I build pipelines, automate things, and occasionally pretend I’m on the VAR panel while yelling at referees from my couch.

So naturally, instead of remembering birthdays like a normal person, I built a serverless reminder system.

## What it does (very basic)

- `DailyBirthdaySummary`: sends birthdays for today
- `MonthlyBirthdaySummary`: sends birthdays for the current month
- test endpoints for both so you can trigger them manually

## Run locally

1. Add your settings in `local.settings.json`
2. Install dependencies:

	`pip install -r requirements.txt`

3. Start the app:

	`func start`

4. Hit test routes:
- `/api/test-daily-summary`
- `/api/test-monthly-summary`

## Important config

- The config pulls from secrets in my azure system, if you are a hacker... then you will neeever get this you will never get this lalalalala (please dont accept the challenge, you guys are scawi)

---

