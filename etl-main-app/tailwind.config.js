/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/main/resources/templates/**/*.html",
    "./src/main/resources/static/**/*.html",
    "../etl-file-engine/src/main/resources/static/**/*.html"
  ],
  theme: {
    extend: {},
  },
  plugins: [],
};
