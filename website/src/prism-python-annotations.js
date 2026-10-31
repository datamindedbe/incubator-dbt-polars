// Prism's Python grammar has no tokens for type hints; highlight them like class names.
import {Prism} from 'prism-react-renderer';

const annotationType = String.raw`[A-Za-z_][\w.]*(?:\[[^\]\n]*\])?`;

Prism.languages.insertBefore('python', 'function', {
  annotation: [
    {
      // def model(...) -> pl.LazyFrame:
      pattern: new RegExp(String.raw`(->[ \t]*)` + annotationType),
      lookbehind: true,
      alias: 'class-name',
    },
    {
      // first: pl.LazyFrame = ...  /  def test(dbt: Dbt, session)
      pattern: new RegExp(
        String.raw`(\b[A-Za-z_]\w*[ \t]*:[ \t]*)` + annotationType + String.raw`(?=[ \t]*(?:=|,|\)))`,
      ),
      lookbehind: true,
      alias: 'class-name',
    },
  ],
});
