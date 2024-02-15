<!-- Text inside of HTML comment blocks will NOT appear in your pull request description -->
<!-- Formatting information can be found at https://www.markdownguide.org/basic-syntax/ -->
# Overview

SNOW-XXXXX

<!--
Why is this review being requested?  The full details should be in the JIRA, but the review should focus on the fix/change being implemented.
If there are multiple steps in the Jira, which step is this?
-->

## Pre-review checklist
- [ ] This change should be part of a Behavior Change Release. See [go/behavior-change](http://go/behavior-change).
- [ ] This change has passed Merge gate tests
- [ ] Snowpipe Changes
- [ ] Snowpipe Streaming Changes
- [ ] This change is TEST-ONLY
- [ ] This change is README/Javadocs only
- [ ] This change is protected by a config parameter <PARAMETER_NAME> eg `snowflake.ingestion.method`.
    - [ ] `Yes` - Added end to end and Unit Tests. 
    - [ ] `No` - Suggest why it is not param protected
- [ ] Is his change protected by parameter <PARAMETER_NAME> on the server side?
    - [ ] The parameter/feature is not yet active in production (partial rollout or PrPr, see [Changes for Unreleased Features and Fixes](http://go/ppp-prpr)).
    - [ ] If there is an issue, it can be safely mitigated by turning the parameter off. This is also verified by a test (See [go/ppp](http://go/ppp)).


<!--
## Urgency
This review is *normal* priority
This review is **high** priority
This review is ***URGENT*** priority
-->

<!--
Indicate any urgency for performing the review.  Perhaps it is a fix for a prod issue or is blocking a customer.
-->

<!--
## Risks
What are the risks associated to your change?
-->

<!--
## Backward and forward Compatible
Imagine customer upgrading to new version and rolling back to older version. Will there be any concerns?
[ ] Backward compatible
[ ] Forward compatible
-->

<!--
Suggested reading order:   Provide an order in which to read files, classes, and methods.  Without this your reader will spend lots of time reading code without understanding the context (imagine the top file references a new class in methods of a file below it).  You need to tell your reviewers how to learn your code.
-->

<!--
## Reviewer roles
Every reviewer must be told their role and what actions are expected of them.  Tell every reviewer what will happen if they don't complete the review.  If you aren't sure who the secondary owner is then work with a manager and/or tech lead to figure this out.
If there are specific items or subsets of the change that you want a particular reviewer to focus on, mention it here.  You can @-mention people using their github username
-->

<!--
Reviewers:  Every review must have at least two reviewers for bug fixes, GA'ed component. One reviewer is enough for test only, doc changes.
Example:
- Minimum # of Required Reviewers - **Two** for Improvements and Bugfixes - <@github alias> 
- Educational purposes - <@github alias>
- Manager/TL approval (If patch critical and requires a release) - <@github alias>
-->

