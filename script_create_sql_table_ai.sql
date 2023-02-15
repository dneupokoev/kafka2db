CREATE TABLE public.ai_linkedid (
	linkedid varchar(64) NOT NULL CONSTRAINT firstkey PRIMARY KEY,
	created_at timestamp NOT NULL DEFAULT NOW(),
	operatorid varchar(255) NOT NULL
);
CREATE INDEX ai_linkedid_created_at_idx ON public.ai_linkedid (created_at);
CREATE INDEX ai_linkedid_operatorid_idx ON public.ai_linkedid (operatorid);
COMMENT ON TABLE public.ai_linkedid IS 'таблица: звонок - оператор';
COMMENT ON COLUMN public.ai_linkedid.linkedid IS 'ссылка на звонок в астериске';
COMMENT ON COLUMN public.ai_linkedid.created_at IS 'дата и время создания текущей записи в БД (не время звонка, а именно когда данная запись создана)';
COMMENT ON COLUMN public.ai_linkedid.operatorid IS 'ссылка на оператора КЦ';


CREATE TABLE public.ai_texts (
	linkedid varchar(64) NOT NULL,
	str varchar(2048) NOT NULL,
	label varchar(64) NOT NULL,
	coefficient numeric(16,15) NOT NULL
);
CREATE INDEX ai_texts_linkedid_idx ON public.ai_texts (linkedid);
CREATE INDEX ai_texts_label_idx ON public.ai_texts (label);
COMMENT ON TABLE public.ai_texts IS 'таблица: звонок - текст - метка - коэффициент вероятности наличия данной метки в данном тексте';
COMMENT ON COLUMN public.ai_texts.linkedid IS 'ссылка на звонок в астериске';
COMMENT ON COLUMN public.ai_texts.str IS 'текст, для которого определяется метка и коэффициент';
COMMENT ON COLUMN public.ai_texts.label IS 'название метки';
COMMENT ON COLUMN public.ai_texts.coefficient IS 'коэффициент вероятности наличия данной метки в данном тексте';


CREATE TABLE public.ai_existence (
	linkedid varchar(255) NOT NULL,
	existence_name varchar(64) NOT NULL,
	is_existence varchar(1) NOT NULL
);
CREATE INDEX ai_existence_linkedid_idx ON public.ai_existence (linkedid);
CREATE INDEX ai_existence_existence_name_idx ON public.ai_existence (existence_name);
CREATE INDEX ai_existence_is_existence_idx ON public.ai_existence (is_existence);
COMMENT ON TABLE public.ai_existence IS 'таблица: звонок - наличие метки в звонке';
COMMENT ON COLUMN public.ai_existence.linkedid IS 'ссылка на звонок в астериске';
COMMENT ON COLUMN public.ai_existence.existence_name IS 'название метки';
COMMENT ON COLUMN public.ai_existence.is_existence IS '1 - метка существует в звонке, 0 - метка не существует в звонке';
