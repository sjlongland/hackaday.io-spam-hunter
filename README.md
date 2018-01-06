Hackaday.io Spam Hunter Project
===============================

The aim of this project is to produce tools that aid in the detection of
spambot user accounts, intended to do little more than spruik some business.

Most of these accounts share common traits that are fairly rudimentary:

- They may feature an avatar with the logo of the company being advertised,
  lots of flat areas of colour, etc.
- They almost certainly give a web address of the business concerned, sometimes
  a phone number or physical address.  Few *real* users do the latter two.
- They often have *followed* a good dozen or more projects in the few minutes
  they have been registered.
- If they publish projects or pages; this content shares the same traits and
  is often posted much faster than the typical human would be able to type.

How this will work
==================

We begin by looking at the full list of users which can be retrieved via the
[users API endpoint](https://dev.hackaday.io/doc/api/get-users).  For the sorts of users we want to target, it looks something like this:

```
{
    "about_me": "<a target=\"_blank\" rel=\"noopener noreferrer\" href=\"http://example.com\">example.com</a>",
    "created": 1515198877,
    "followers": 1,
    "following": 1,
    "id": 123456789,
    "image_url": "https://cdn.hackaday.io/images/default-avatar.png",
    "location": "",
    "projects": 0,
    "rank": 1000000,
    "screen_name": "aspamuser",
    "skulls": 0,
    "tags": null,
    "url": "https://hackaday.io/aspamuser",
    "username": "aspamuser",
    "what_i_have_done": "",
    "what_i_would_like_to_do": "",
    "who_am_i": ""
}
```

or sometimes the account is benign like this:

```
{
    "about_me": "how to hack into someones snapchat",
    "created": 1515199252,
    "followers": 1,
    "following": 1,
    "id": 12345678,
    "image_url": "https://cdn.hackaday.io/images/default-avatar.png",
    "location": "",
    "projects": 0,
    "rank": 1000000,
    "screen_name": "aspamuser",
    "skulls": 0,
    "tags": null,
    "url": "https://hackaday.io/aspamuser",
    "username": "aspamuser",
    "what_i_have_done": "",
    "what_i_would_like_to_do": "",
    "who_am_i": ""
}
```
… but then it has links elsewhere:

```
{
    "last_page": 1,
    "links": [
        {
            "id": 12345678,
            "title": "how to hack into someones snapchat",
            "type": "other",
            "url": "https://example.com/"
        }
    ],
    "page": 1,
    "per_page": 1,
    "total": 1
}
```

Based on this, the `about_me`, `who_am_i` and links are definite places we can
be looking to identify such users.

The first step will be to grab the information from the API and cache it
temporarily, probably in RAM since we don't want to keep it long-term, and pick
out those accounts that have string patterns that match URIs, telephone
numbers or physical addresses.

For the sake of not repeating ourselves, we should persistently store at least
the profile IDs of users we have "seen" already, as there's a good chance of false
positives in that.

A human can then decide whether the user is genuine or not, and the record
updated accordingly, if not genuine, they can then proceed to the profile page
to report the user.  This will likely require oAuth authentication and require
the user to be "joined" to this project.

What this project is not
========================

- We won't be "automatically" banning users or filing spam reports in any sort
  of automated fashion.
- We will *not* be undertaking in any vigilante action: the aim here is to
  identify the accounts so they can be removed.  If SupplyFrame decide to take
  action against the business concerned, that is their decision to make, not
  ours.
