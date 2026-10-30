import { defineConfig } from "oxlint";
import {
  ANTELOPE_IGNORE_PATTERNS,
  antelopePreset,
} from "@antelopejs/tooling-configs/oxc/lint";

export default defineConfig({
  extends: [antelopePreset()],
  ignorePatterns: ANTELOPE_IGNORE_PATTERNS,
  options: { typeAware: true },
});
