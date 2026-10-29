name: Noon UAE – Lab Ranking Test (Ubicloud EU)
run-name: Noon lab test ${{ inputs.test_id }} m${{ inputs.machine_no }} (EU)

# Same as noon-uae-lab-ranking.yml but runs on Ubicloud (European IP, no proxy).
# Tests whether European datacenter IPs bypass Noon's peak-hour blocking.

on:
  workflow_dispatch:
    inputs:
      test_id:
        description: 'noon_lab.test.test_id'
        required: true
        type: string
      machine_no:
        description: 'noon_lab.machine.machine_no'
        required: true
        type: string

concurrency:
  group: noon-lab-${{ inputs.test_id }}-m${{ inputs.machine_no }}
  cancel-in-progress: false

jobs:
  probe:
    runs-on: ubicloud-standard-2
    timeout-minutes: 15
    name: test ${{ inputs.test_id }} machine ${{ inputs.machine_no }}
    steps:
      - uses: actions/checkout@v4
        with:
          sparse-checkout: noon/uae
      - uses: actions/setup-node@v4
        with:
          node-version: '22'
      - name: Run lab worker
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_ROLE_KEY: ${{ secrets.SUPABASE_SERVICE_ROLE_KEY }}
          TEST_ID: ${{ inputs.test_id }}
          MACHINE_NO: ${{ inputs.machine_no }}
          GITHUB_RUN_ID: ${{ github.run_id }}
        run: |
          npm install undici
          node noon/uae/lab-ranking.js
