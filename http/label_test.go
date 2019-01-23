package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	http "net/http"
	"net/http/httptest"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	platformtesting "github.com/influxdata/influxdb/testing"
	"github.com/julienschmidt/httprouter"
)

func TestService_handleGetLabels(t *testing.T) {
	type fields struct {
		LabelService platform.LabelService
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		wants  wants
	}{
		{
			name: "get all labels",
			fields: fields{
				&mock.LabelService{
					FindLabelsFn: func(ctx context.Context, filter platform.LabelFilter) ([]*platform.Label, error) {
						return []*platform.Label{
							{
								ID:   platformtesting.MustIDBase16("0b501e7e557ab1ed"),
								Name: "hello",
								Properties: map[string]string{
									"color": "fff000",
								},
							},
							{
								ID:   platformtesting.MustIDBase16("c0175f0077a77005"),
								Name: "example",
								Properties: map[string]string{
									"color": "fff000",
								},
							},
						}, nil
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/labels"
  },
  "labels": [
    {
      "id": "0b501e7e557ab1ed",
      "name": "hello",
      "properties": {
        "color": "fff000"
      }
    },
    {
      "id": "c0175f0077a77005",
      "name": "example",
      "properties": {
        "color": "fff000"
      }
    }
  ]
}
`,
			},
		},
		{
			name: "get all labels when there are none",
			fields: fields{
				&mock.LabelService{
					FindLabelsFn: func(ctx context.Context, filter platform.LabelFilter) ([]*platform.Label, error) {
						return []*platform.Label{}, nil
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/labels"
  },
  "labels": []
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewLabelHandler()
			h.LabelService = tt.fields.LabelService

			r := httptest.NewRequest("GET", "http://any.url", nil)

			w := httptest.NewRecorder()

			h.handleGetLabels(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetLabels() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetLabels() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil || tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetLabels() = ***%v***", tt.name, diff)
			}
		})
	}
}

func TestService_handleGetLabel(t *testing.T) {
	type fields struct {
		LabelService platform.LabelService
	}
	type args struct {
		id string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "get a label by id",
			fields: fields{
				&mock.LabelService{
					FindLabelByIDFn: func(ctx context.Context, id platform.ID) (*platform.Label, error) {
						if id == platformtesting.MustIDBase16("020f755c3c082000") {
							return &platform.Label{
								ID:   platformtesting.MustIDBase16("020f755c3c082000"),
								Name: "mylabel",
								Properties: map[string]string{
									"color": "fff000",
								},
							}, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/labels/020f755c3c082000"
  },
  "label": {
    "id": "020f755c3c082000",
    "name": "mylabel",
    "properties": {
      "color": "fff000"
    }
  }
}
`,
			},
		},
		{
			name: "not found",
			fields: fields{
				&mock.LabelService{
					FindLabelByIDFn: func(ctx context.Context, id platform.ID) (*platform.Label, error) {
						return nil, &platform.Error{
							Code: platform.ENotFound,
							Err:  platform.ErrLabelNotFound,
						}
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewLabelHandler()
			h.LabelService = tt.fields.LabelService

			r := httptest.NewRequest("GET", "http://any.url", nil)

			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			h.handleGetLabel(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetLabel() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetLabel() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetLabel() = ***%v***", tt.name, diff)
			}
		})
	}
}

func TestService_handlePostLabel(t *testing.T) {
	type fields struct {
		LabelService platform.LabelService
	}
	type args struct {
		label *platform.Label
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "create a new label",
			fields: fields{
				&mock.LabelService{
					CreateLabelFn: func(ctx context.Context, l *platform.Label) error {
						l.ID = platformtesting.MustIDBase16("020f755c3c082000")
						return nil
					},
				},
			},
			args: args{
				label: &platform.Label{
					Name: "mylabel",
				},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/labels/020f755c3c082000"
  },
  "label": {
    "id": "020f755c3c082000",
    "name": "mylabel"
  }
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewLabelHandler()
			h.LabelService = tt.fields.LabelService

			l, err := json.Marshal(tt.args.label)
			if err != nil {
				t.Fatalf("failed to marshal label: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(l))
			w := httptest.NewRecorder()

			h.handlePostLabel(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePostLabel() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostLabel() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil || tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePostLabel() = ***%v***", tt.name, diff)
			}
		})
	}
}

func TestService_handleDeleteLabel(t *testing.T) {
	type fields struct {
		LabelService platform.LabelService
	}
	type args struct {
		id string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "remove a label by id",
			fields: fields{
				&mock.LabelService{
					DeleteLabelFn: func(ctx context.Context, id platform.ID) error {
						if id == platformtesting.MustIDBase16("020f755c3c082000") {
							return nil
						}

						return fmt.Errorf("wrong id")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusNoContent,
			},
		},
		{
			name: "label not found",
			fields: fields{
				&mock.LabelService{
					DeleteLabelFn: func(ctx context.Context, id platform.ID) error {
						return &platform.Error{
							Code: platform.ENotFound,
							Err:  platform.ErrLabelNotFound,
						}
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewLabelHandler()
			h.LabelService = tt.fields.LabelService

			r := httptest.NewRequest("GET", "http://any.url", nil)

			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			h.handleDeleteLabel(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePostLabel() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostLabel() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePostLabel() = ***%v***", tt.name, diff)
			}
		})
	}
}

func TestService_handlePatchLabel(t *testing.T) {
	type fields struct {
		LabelService platform.LabelService
	}
	type args struct {
		id         string
		properties map[string]string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "update label properties",
			fields: fields{
				&mock.LabelService{
					UpdateLabelFn: func(ctx context.Context, id platform.ID, upd platform.LabelUpdate) (*platform.Label, error) {
						if id == platformtesting.MustIDBase16("020f755c3c082000") {
							l := &platform.Label{
								ID:   platformtesting.MustIDBase16("020f755c3c082000"),
								Name: "mylabel",
								Properties: map[string]string{
									"color": "fff000",
								},
							}

							for k, v := range upd.Properties {
								if v == "" {
									delete(l.Properties, k)
								} else {
									l.Properties[k] = v
								}
							}

							return l, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
				properties: map[string]string{
					"color": "aaabbb",
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/labels/020f755c3c082000"
  },
  "label": {
    "id": "020f755c3c082000",
    "name": "mylabel",
		"properties": {
			"color": "aaabbb"
		}
  }
}
`,
			},
		},
		{
			name: "label not found",
			fields: fields{
				&mock.LabelService{
					UpdateLabelFn: func(ctx context.Context, id platform.ID, upd platform.LabelUpdate) (*platform.Label, error) {
						return nil, &platform.Error{
							Code: platform.ENotFound,
							Err:  platform.ErrLabelNotFound,
						}
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
				properties: map[string]string{
					"color": "aaabbb",
				},
			},
			wants: wants{
				statusCode: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewLabelHandler()
			h.LabelService = tt.fields.LabelService

			upd := platform.LabelUpdate{}
			if len(tt.args.properties) > 0 {
				upd.Properties = tt.args.properties
			}

			l, err := json.Marshal(upd)
			if err != nil {
				t.Fatalf("failed to marshal label update: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(l))

			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			h.handlePatchLabel(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePatchLabel() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePatchLabel() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePatchLabel() = ***%v***", tt.name, diff)
			}
		})
	}
}
