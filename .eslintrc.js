module.exports = {
  parser: '@typescript-eslint/parser',
  parserOptions: {
    project: 'tsconfig.json',
    sourceType: 'module',
  },
  plugins: ['@typescript-eslint/eslint-plugin'],
  extends: [
    'plugin:@typescript-eslint/recommended',
  ],
  root: true,
  env: {
    node: true,
    jest: true,
  },
  rules: {
    "@typescript-eslint/no-inferrable-types": ["off"],
    "max-len": ["off"],
    "semi": ["error"],
    "comma-dangle": ["error", "always-multiline"],
    "@typescript-eslint/no-explicit-any": ["off"],
    "@typescript-eslint/ban-types": ["off"]
  },
  ignorePatterns: ['.eslintrc.js'],
};