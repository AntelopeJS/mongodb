---
title: "Meter"
description: An x-of-y bar for seats, quotas and coverage. It draws one fill or stacked segments, with warning and error thresholds.
mode: reference
---

`Meter` draws an "x of y" bar: the label, a dimmed hint and the value on one line, the bar under it, then an optional legend. One value fills the bar in its tone; `segments` stack several fills, each with its legend label. `warnAt` and `errorAt` turn a single fill warning or error past a percentage of `max`.

::block-playground{type="Meter" height="160"}
---
options:
  label: Seats
  hint: 8 in use · 2 free
  max: 10
  legend: true
  card: true
  segments:
    - value: 6
      label: 6 members
    - value: 2
      tone: soft
      label: 2 pending invites
  actions:
    - label: Manage members
      to: /settings/members
---
::
