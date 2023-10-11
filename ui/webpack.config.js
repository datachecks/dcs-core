const path = require("path");
const TerserPlugin = require("terser-webpack-plugin");

module.exports = {
  entry: "./src/index.tsx",
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        use: [{ loader: "ts-loader" }],
        exclude: /node_modules/,
      },
      {
        test: /\.css$/,
        use: [
          { loader: "style-loader" },
          {
            loader: "css-loader",
            options: {
              modules: true,
            },
          },
        ],
        exclude: /node_modules/,
      },
      {
        test: /\.svg$/,
        use: [{ loader: "svg-inline-loader" }],
        exclude: /node_modules/,
      },
      {
        test: /\.(woff|woff2|ttf|eot)$/,
        use: {
          loader: "file-loader",
          options: {
            name: "assets/fonts/[name].[ext]",
          },
        },
      },
    ],
  },
  resolve: {
    fallback: {
      buffer: require.resolve("buffer/"),
      path: false,
      fs: false,
    },
    extensions: [".tsx", ".ts", ".js"],
    alias: {
      assets: path.resolve(__dirname, "../datachecks/report/static/assets/"),
    },
  },
  output: {
    filename: "index.js",
    path: path.resolve(__dirname, "../datachecks/report/static/"),
    publicPath: path.resolve(__dirname, "../datachecks/report/static//"),
  },
  optimization: {
    minimize: true,
    minimizer: [
      new TerserPlugin({
        terserOptions: {
          output: {
            comments: false,
          },
        },
      }),
    ],
  },
};
