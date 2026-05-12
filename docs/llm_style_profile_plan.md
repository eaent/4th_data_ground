# LLM JSON Mode 스타일 프로파일러 구현 계획

## 목표

상품/향 설명 텍스트를 LLM JSON mode로 호출해 아래 스키마로 분류한다.

```json
{
  "style": "...",
  "mood": "...",
  "color": "..."
}
```

## 구현 범위

1. `style`, `mood`, `color` Pydantic 스키마 정의
2. 7개 스타일 카테고리 기반 few-shot 프롬프트 작성
3. OpenAI Chat Completions JSON mode 호출
4. JSON 파싱 실패 또는 Pydantic 검증 실패 시 최대 3회 재시도
5. API key 없이도 검증 가능한 fake client 테스트 구조 제공

## 7개 스타일 카테고리

- `fresh_clean`: 산뜻함, 깨끗함, 시트러스/화이트 머스크 계열
- `floral_romantic`: 플로럴, 부드러움, 로맨틱 계열
- `woody_earthy`: 우디, 흙내음, 차분한 자연감
- `amber_warm`: 앰버, 바닐라, 따뜻하고 관능적인 계열
- `spicy_oriental`: 스파이시, 오리엔탈, 이국적이고 강렬한 계열
- `gourmand_sweet`: 달콤함, 디저트/캔디/크리미 계열
- `aquatic_marine`: 물, 바다, 투명하고 시원한 계열

## 파일 배치

- 구현: `models/style_profile_llm.py`
- 계획: `docs/llm_style_profile_plan.md`

## 검증

- `python -m py_compile models/style_profile_llm.py`
- fake client로 재시도 및 Pydantic 검증 동작 확인
