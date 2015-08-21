# slack-archv
A little project written in Python to make your Slack archive painlessly.

# Why Slack Archv?
One of the annoying issue on Slack is the number of searchable message is only 10,000 for teams on free plan. Beyond this number, messages are unreachable even from APIs.
As we wanted to keep a full history of our team for easy access, we started this project as more and more messages sending into Slack by people.

# Version
Current **v1.0.0 RC1**.

The table schema can be consider stable, but the mechanism is not production-ready. You can try making archives, but the correctness and integrity is not guaranteed.

# Features
1. Very Python!
2. All history organized **in a SQLite database**, including all the messages, files and attachments.
3. A snapshot of channels, users and emojis is kept for easily access by joining tables.
4. Data is saved fetched with only a Slack API token. No integration is required.
5. Only public history is stored, and we are going to develop a option to archive messages from private groups or direct messages as well.
6. Messages get updated if they were edited after the last query.
7. Reactions and star list of all team members are also included!

# License
The project is [licensed under MIT](LICENSE).

# Contributions
We are open to any ideas on the project. Issues and PRs are always welcome.
