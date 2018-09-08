"use strict";

/*! All users currently being managed */
var users = {};

/*! All words being managed */
var words = {};

/*! Word Adjacencies being managed */
var wordadj = {};

/*! Hostnames being managed */
var hostnames = {};

/*! Groups being managed */
var groups = {};

/* Pagination and source */
var source = '/data/newcomers.json',
	newest_uid = null,
	oldest_uid = null;

/* UI state */
var busy = false;

/* UI panes and controls */
var title_pane = null,
	user_pane = null,
	status_pane = null,
	user_uis = [],
	selected_uid = null,
	prev_selected_uid = null,
	heading = null;

/* Pending user actions */
var user_actions = {};

/* Credit: https://stackoverflow.com/a/7124052 */
var htmlEscape = function(str) {
    return str
        .replace(/&/g, '&amp;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
};

/*!
 * Clear the given DOM element of children.  Return those children.
 */
const clear_element = function(element) {
	const children = [].slice.apply(element.childNodes);
	children.forEach((c) => {
		element.removeChild(c);
	});
	return children;
};

/*!
 * Configure a XMLHttpRequest with a call-back function
 * for use with Promises.
 */
const setup_xhr = function(rq, resolve, reject) {
	rq.onreadystatechange = function() {
		if (rq.readyState == 4) {
			if (rq.status === 200) {
				resolve(rq);
			} else {
				const err = new Error('Request failed');
				err.rq = rq;
				reject(err);
			}
		}
	}
}

/*!
 * Perform a generic HTTP request
 */
const promise_http = function(uri, method, body_ct, body) {
	return new Promise(function (resolve, reject) {
		const rq = new XMLHttpRequest();
		setup_xhr(rq, resolve, reject);
		rq.open(method || 'GET', uri, true);
		if (body_ct && body) {
			rq.setRequestHeader("Content-type", body_ct);
			rq.send(body);
		} else {
			rq.send();
		}
	});
};

/*!
 * Retrieve JSON via HTTP using Promises
 */
const get_json = function(uri) {
	return promise_http(uri, 'GET').then(function (res) {
		return JSON.parse(res.responseText);
	});
};

/*!
 * Send JSON via HTTP using Promises
 */
const post_json = function(uri, data) {
	return promise_http(uri, 'POST',
		'application/json', JSON.stringify(data)
	).then(function (res) {
		if (res.responseText)
			return JSON.parse(res.responseText)
	});
};

/*!
 * A mathematical set
 */
const Set = function() {
	const self = this;
	this._elements = {};
	([].slice.apply(arguments)).forEach((e) => {
		self._elements[e] = true;
	});
};

Set.prototype.has = function(e) {
	return this._elements.hasOwnProperty(e);
};

Set.prototype.add = function() {
	const self = this;
	([].slice.apply(arguments)).forEach((e) => {
		self._elements[e] = true;
	});
};

Set.prototype.union = function() {
	const self = this;
	let res = self.clone();
	([].slice.apply(arguments)).forEach((s) => {
		res.add.apply(s.elements());
	});
	return res;
};

Set.prototype.rm = function() {
	const self = this;
	([].slice.apply(arguments)).forEach((e) => {
		if (self._elements.hasOwnProperty(e))
			delete self.elements[e];
	});
};

Set.prototype.elements = function() {
	return Object.keys(this._elements);
};

Set.prototype.clone = function() {
	const self = this;
	let clone = new Set();
	clone.add.apply(self.elements());
	return clone;
};

Set.prototype.size = function() {
	return Object.keys(this._elements).length;
};

const ObjectSet = function() {
	const self = this;
	let args = ([].slice.apply(arguments));
	this._class = args.shift();
	this._collection = args.shift();
	this._key_fn = args.shift();
	this._obj_key = (e) => {
		if (!(e instanceof self._class))
			throw new Error('Incorrect argument type');
		return self._key_fn(e);
	};

	Set.apply(this, args.map(self._obj_key));
}

ObjectSet.prototype = Object.create(Set.prototype);

ObjectSet.prototype.has = function(e) {
	const key = this._obj_key(e);

	if (!this._collection.hasOwnProperty(key)) {
		Set.prototype.rm.call(this, key);
		return false;
	}

	return Set.prototype.has.call(this, key);
};

ObjectSet.prototype.add = function() {
	const self = this,
		args = [].slice.apply(arguments);
	Set.prototype.add.apply(self,
		args.map((e) => {
			const key = self._obj_key(e);
			if (!self._collection.hasOwnProperty(key))
				throw new Error('Invalid instance for set: '
					+ key);
			return key;
		}));
};

ObjectSet.prototype.rm = function() {
	const self = this,
		args = [].slice.apply(arguments);
	Set.prototype.rm.apply(self, args.map(self._obj_key));
};

ObjectSet.prototype.elements = function() {
	const self = this;
	return Object.keys(this._elements).filter((key) => {
		if (!self._collection.hasOwnProperty(key)) {
			Set.prototype.rm.call(self, key);
			return false;
		}
		return true;
	}).map((key) => {
		return self._collection[key];
	});
};

ObjectSet.prototype.clone = function() {
	const self = this;
	let clone = new ObjectSet(self._class,
		self._collection, self._key_fn);
	clone.add.apply(self.elements());
	return clone;
};

const UserSet = function() {
	let args = ([].slice.apply(arguments));
	args.unshift((user) => {
		return user.id;
	});
	args.unshift(users);
	args.unshift(User);
	ObjectSet.apply(this, args);
};
UserSet.prototype = Object.create(ObjectSet.prototype);

const WordSet = function() {
	let args = ([].slice.apply(arguments));
	args.unshift((word) => {
		return word.id;
	});
	args.unshift(words);
	args.unshift(Word);
	ObjectSet.apply(this, args);
};

WordSet.prototype = Object.create(ObjectSet.prototype);

const WordAdjSet = function() {
	let args = ([].slice.apply(arguments));
	args.unshift((wa) => {
		return wa.key;
	});
	args.unshift(wordadj);
	args.unshift(WordAdj);
	ObjectSet.apply(this, args);
};

WordAdjSet.prototype = Object.create(ObjectSet.prototype);

const HostnameSet = function() {
	let args = ([].slice.apply(arguments));
	args.unshift((h) => {
		return h.id;
	});
	args.unshift(hostnames);
	args.unshift(Hostname);
	ObjectSet.apply(this, args);
};

HostnameSet.prototype = Object.create(ObjectSet.prototype);

const GroupSet = function() {
	let args = ([].slice.apply(arguments));
	args.unshift((g) => {
		return g.name;
	});
	args.unshift(groups);
	args.unshift(Group);
	ObjectSet.apply(this, args);
};

GroupSet.prototype = Object.create(ObjectSet.prototype);

/*!
 * A scored object.
 */
const ScoredObject = function(score, count) {
	this.users = new UserSet();
	this.ui = {};
	this.update_score(score, count);
};

ScoredObject.sortByScore = function(a, b) {
	if (a.normalised_score < b.normalised_score)
		return -1;
	if (a.normalised_score > b.normalised_score)
		return 1;
	return 0;
};

ScoredObject.prototype._get_normalised_score = function() {
	if (this.count === 0)
		return 0;

	return (Math.round((100 * this.score)
		/ this.count)
		/ 100.0);
};

ScoredObject.prototype.update_score = function(score, count) {
	const self = this;
	self.score = score;
	self.count = count;
	self.normalised_score = self._get_normalised_score();

	self.users.elements().forEach((u) => {
		u.update_score();
	});
	Object.keys(self.ui).forEach((id) => {
		self.ui[id].update(self);
	});
};

/*!
 * A word used by one or more users.
 */
const Word = function(id, word, score, count) {
	if (words.hasOwnProperty(id))
		throw new Error('Existing word');

	words[id] = this;

	this.id = id;
	this.word = word;
	this.wordadj = new WordAdjSet();
	ScoredObject.call(this, score, count);
};
Word.prototype = Object.create(ScoredObject.prototype);

Word.from_data = function(word, data) {
	let w = words[data.id];
	if (w) {
		w.update_score(
			data.score || data.site_score,
			data.count || data.site_count);
	} else {
		w = new Word(data.id, word,
			data.score || data.site_score,
			data.count || data.site_count);
	}
	return w;
};

Word.from_id_name = function(id, word) {
	let w = words[id];
	if (!w) {
		w = new Word(id, word);
	}
	return w;
};

Word.prototype.destroy = function() {
	const self = this;

	delete words[self.id];

	self.users.elements().forEach((u) => {
		u.words.rm(self);
	});
	self.wordadj.elements().forEach((wa) => {
		wa.destroy();
	});
};

/*!
 * A pair of words that are adjacent.
 */
const WordAdj = function(proceeding, following, score, count) {
	const key = WordAdj._key_from_ids(proceeding.id, following.id);

	if (wordadj.hasOwnProperty(key))
		throw new Error('Existing word adjacency');

	wordadj[key] = this;

	this.proceeding_id = proceeding.id;
	this.following_id = following.id;
	this.key = key;
	this.score = score;
	this.count = count;
	this.users = new UserSet();

	proceeding.wordadj.add(this);
	following.wordadj.add(this);
	ScoredObject.call(this, score, count);
};
WordAdj.prototype = Object.create(ScoredObject.prototype);

WordAdj._key_from_ids = function(proceeding_id, following_id) {
	return JSON.stringify([proceeding_id, following_id]);
};

WordAdj.from_data = function(data) {
	const key = WordAdj._key_from_ids(data.proceeding_id,
					data.following_id);
	let wa = wordadj[key];
	if (wa) {
		wa.update_score(
			data.score || data.site_score,
			data.count || data.site_count);
	} else {
		wa = new WordAdj(
			Word.from_id_name(data.proceeding_id),
			Word.from_id_name(data.following_id),
			data.score || data.site_score,
			data.count || data.site_count);
	}
	return wa;
};

WordAdj.prototype.proceeding = function() {
	return words[this.proceeding_id];
};

WordAdj.prototype.following = function() {
	return words[this.following_id];
};

WordAdj.prototype.destroy = function() {
	const self = this;

	delete wordadj[self.key];

	self.users.elements().forEach((u) => {
		u.wordadj.rm(self);
	});

	if (words.hasOwnProperty(self.proceeding_id))
		words[self.proceeding_id].wordadj.rm(self);

	if (words.hasOwnProperty(self.following_id))
		words[self.following_id].wordadj.rm(self);
};

/*!
 * A hostname used by one or more users.
 */
const Hostname = function(id, hostname, score, count) {
	if (hostnames.hasOwnProperty(id))
		throw new Error('Existing hostname');

	hostnames[id] = this;

	this.id = id;
	this.hostname = hostname;
	ScoredObject.call(this, score, count);
};
Hostname.prototype = Object.create(ScoredObject.prototype);

Hostname.from_data = function(hostname, data) {
	let h = hostnames[data.id];
	if (h) {
		h.update_score(
			data.score || data.site_score,
			data.count || data.site_count);
	} else {
		h = new Hostname(data.id, hostname,
			data.score || data.site_score,
			data.count || data.site_count);
	}
	return h;
};

Hostname.from_id_name = function(id, hostname) {
	let h = hostnames[id];
	if (!h) {
		h = new Hostname(id, hostname);
	}
	return h;
};

Hostname.prototype.destroy = function() {
	const self = this;

	delete hostnames[self.id];

	self.users.elements().forEach((u) => {
		u.hostnames.rm(self);
	});
};

/*!
 * A group of users
 */
const Group = function(name) {
	if (groups.hasOwnProperty(name))
		throw new Error('Existing group');

	groups[name] = this;

	this.name = name;
	this.members = new UserSet();
};

Group.get = function(name) {
	let g = groups[name];
	if (!g) {
		g = new Group(name);
	}
	return g;
};

/*!
 * A user returned by the API
 */
const User = function(data) {
	const self = this;

	if (users.hasOwnProperty(data.id))
		throw new Error('Existing user');

	users[data.id] = this;

	self.id = data.id;
	self.groups = new GroupSet();
	self.hostnames = new HostnameSet();
	self.words = new WordSet();
	self.wordadj = new WordAdjSet();
	self.update(data);

	self.ui = null;
	self._action = null;
	self._score = null;
};

User.from_data = function(data) {
	let u = users[data.id];
	if (u)
		u.update(data);
	else
		u = new User(data);
	return u;
};

User.prototype._calc_score = function() {
	let user_score = [];

	this.hostnames.elements().forEach((hostname) => {
		user_score.push(hostname.normalised_score);
	});

	this.words.elements().forEach((word) => {
		user_score.push(word.normalised_score);
	});

	this.wordadj.elements().forEach((wordadj) => {
		user_score.push(wordadj.normalised_score);
	});

	/* Compute user score */
	if (user_score.length) {
		return Math.round(100*user_score.sort(function (a, b) {
			if (a < b)
				return -1;
			else if (a > b)
				return 1;
			return 0;
		}).slice(0, 5).reduce(function (a, b) {
			return a + b;
		})) / 100;
	} else {
		return 0.0;
	}
};

User.prototype.action = function() {
	return this._action;
};

User.prototype.set_action = function(action) {
	if (this._action === action)
		return;

	if ((action === 'suspect')
			|| (action === null)
			|| (action === 'legit')) {
		this._action = action;
		if (action === null) {
			if (user_actions.hasOwnProperty(this.id))
				delete user_actions[this.id];
		} else {
			user_actions[this.id] = action;
		}

		update_pending_actions();
	} else {
		throw new Error('Invalid action');
	}
};

User.prototype.refresh = function() {
	return get_json('/user/' + this.id).then((data) => {
		return this.update(data);
	});
};

User.prototype.commit = function() {
	const self = this;

	if (self._action === null)
		return Promise.resolve();
	else
		return post_json(
			'/classify/' + self.id,
			self._action
		).then(() => {
			return self.refresh();
		}).then(() => {
			if (user_actions[self.id] === self._action) {
				self.set_action(null);
			}
		});
};

User.prototype.score = function() {
	if (this._score === null) {
		this._score = this._calc_score();
	}
	return this._score;
};

User.prototype.update_score = function() {
	this._score = null;
	if (this.ui) {
		this.ui.refresh();
	}
};

User.prototype.update = function(data) {
	const self = this;

	if (data.id !== self.id)
		throw new Error('Mismatched user ID');

	self.ui = null;
	self.screen_name = data.screen_name;
	self.location = data.location;
	self.about_me = data.about_me;
	self.who_am_i = data.who_am_i;
	self.what_i_would_like_to_do = data.what_i_would_like_to_do;
	self.tags = data.tags;
	self.links = data.links;
	self.projects = data.projects;
	self.avatar_id = data.avatar_id;
	self.created = data.created;
	self.had_created = data.had_created;
	self.last_update = data.last_update;
	self.tokens = data.tokens;
	self.next_inspection = data.next_inspection;
	self.inspections = data.inspections;
	self.pending = data.pending;
	self.url = data.url;

	let in_group = {};
	data.groups.forEach((name) => {
		const g = Group.get(name);
		in_group[name] = true;
		g.members.add(self);
		self.groups.add(g);
	});
	self.groups.elements().forEach((g) => {
		if (!in_group[g.name]) {
			self.groups.rm(g);
			g.members.rm(self);
		}
	});

	let seen_hostnames = {};
	Object.keys(data.hostnames).forEach((hostname) => {
		const hd = data.hostnames[hostname];
		const h = Hostname.from_data(hostname, hd);
		seen_hostnames[h.id] = hd.user_count || 0;
		h.users.add(self);
		self.hostnames.add(h);
	});
	self.hostnames.elements().forEach((h) => {
		if (!seen_hostnames[h.id]) {
			self.hostnames.rm(h);
			h.users.rm(self);
		}
	});
	self.hostname_count = seen_hostnames;

	let seen_word = {};
	Object.keys(data.words).forEach((word) => {
		const wd = data.words[word];
		const w = Word.from_data(word, wd);
		seen_word[w.id] = wd.user_count || 0;
		w.users.add(self);
		self.words.add(w);
	});
	self.words.elements().forEach((w) => {
		if (!seen_word[w.id]) {
			self.words.rm(w);
			w.users.rm(self);
		}
	});
	self.word_count = seen_word;

	let seen_wordadj = {};
	data.word_adj.forEach((wordadj) => {
		const wa = WordAdj.from_data(wordadj);
		seen_wordadj[wa.key] = wa.user_count;
		wa.users.add(self);
		self.wordadj.add(wa);
	});
	self.wordadj.elements().forEach((wa) => {
		if (!seen_wordadj[wa.key]) {
			self.wordadj.rm(wa);
			wa.users.rm(self);
		}
	});
	self.wordadj_count = seen_wordadj;
};

/*!
 * DOM Element helper
 */
const DOMElement = function(type, properties) {
	const self = this;
	this.element = document.createElement(type);
	if (properties) {
		Object.keys(properties).forEach((p) => {
			const val = properties[p];
			const set_fn = self['set_' + p];

			if ((typeof set_fn) === 'function')
				set_fn.call(self, val, p);
			else
				self.element[p] = val;
		});
	}
};

DOMElement.prototype.set_classes = function(classes) {
	const self = this;
	let seen = new Set();
	classes.forEach((c) => {
		self.element.classList.add(c);
		seen.add(c);
	});
	[].slice.apply(self.element.classList).forEach((c) => {
		if (!seen.has(c))
			self.element.classList.remove(c);
	});
};

DOMElement.prototype.add_children = function() {
	const self = this;
	const args = [].slice.apply(arguments);

	let pos = 'end', target = null, is_target = false;

	const res = args.map((c) => {
		if ((typeof c) === 'string') {
			/*
			 * Position hint.  If this is 'before',
			 * this will be followed up by another element.
			 */
			pos = c;
			is_target = (c === 'before');
			return null;
		}

		const e = (c instanceof DOMElement) ? c.element : c;
		if (is_target) {
			target = e;
			is_target = false;
			return null;
		}

		switch (pos) {
		case 'start':
			if (self.element.childNodes.length) {
				/* Subsequent insertions will happen here */
				target = self.element.childNodes[0];
				pos = 'before';
				self.element.insertBefore(e, target);
			} else {
				/* This is the only element */
				self.element.appendChild(e);
				pos = 'end';
			}
			break;
		case 'before':
			self.element.insertBefore(e, target);
			break;
		case 'end':
		default:
			self.element.appendChild(e);
		}
		return c;
	}).filter((c) => {
		return c !== null;
	});
	if (args.length === 1) {
		return res[0];
	} else {
		return res;
	}
};

DOMElement.prototype.add_text = function(text) {
	return this.add_children(
		document.createTextNode(text)
	);
};

DOMElement.prototype.add_new_child = function(type, properties) {
	return this.add_children(new DOMElement(type, properties));
};

DOMElement.prototype.remove_children = function() {
	const self = this;
	const args = [].slice.apply(arguments);
	args.forEach((c) => {
		self.element.removeChild(
			(c instanceof DOMElement)
			? c.element
			: c
		);
	});
};

DOMElement.prototype.rm_classes = function() {
	const self = this;
	[].slice.apply(arguments).forEach((c) => {
		if (self.element.classList.contains(c))
			self.element.classList.remove(c);
	});
};

DOMElement.prototype.add_classes = function() {
	const self = this;
	[].slice.apply(arguments).forEach((c) => {
		if (!self.element.classList.contains(c))
			self.element.classList.add(c);
	});
};

DOMElement.prototype.swap_class = function(rm_class, add_class) {
	if (rm_class instanceof Array)
		this.rm_classes.apply(this, rm_class);
	else
		this.rm_classes(rm_class);

	if (add_class instanceof Array)
		this.add_classes.apply(this, add_class);
	else
		this.add_classes(add_class);
};

DOMElement.prototype.clear = function() {
	const self = this;
	self.remove_children.apply(self,
		[].slice.apply(self.element.childNodes));
};

DOMElement.prototype.destroy = function() {
	this.clear();
	if (this.element.parentElement)
		this.element.parentElement.removeChild(this.element);
};

/*!
 * Score gauge UI control
 */
const ScoreGauge = function(score) {
	this.gaugeBox = new DOMElement('div', {
		classes: ['score_gauge', 'score_gauge_base']
	});
	this.gaugeLeft = this.gaugeBox.add_new_child('div', {
		classes: ['score_gauge', 'score_gauge_indication']
	});
	this.gaugeBar = this.gaugeBox.add_new_child('div', {
		classes: ['score_gauge', 'score_gauge_indication']
	});
	this.gaugeRight = this.gaugeBox.add_new_child('div', {
		classes: ['score_gauge', 'score_gauge_indication']
	});

	this.set(score);
};

ScoreGauge.prototype.set = function(score) {
	if (score < 0.0) {
		this.gaugeLeft.element.style.width = (16 * (10.0 + (2*score))) + 'px';
		this.gaugeBar.element.style.width = (16 * (-(2*score))) + 'px';
		this.gaugeRight.element.style.width = '160px';
	} else if (score > 0.0) {
		this.gaugeLeft.element.style.width = '160px';
		this.gaugeBar.element.style.width = (16 * (2*score)) + 'px';
		this.gaugeRight.element.style.width = (16 * (10.0 - (2*score))) + 'px';
	} else {
		this.gaugeLeft.element.style.width = '155px';
		this.gaugeBar.element.style.width = '10px';
		this.gaugeRight.element.style.width = '155px';
	}
	this.gaugeBar.element.style.backgroundColor = scoreColour(score)
};

/*!
 * Scored Object UI element
 */
const ScoredObjectUI = function(oid, user_count) {
	const self = this;
	self.id = oid;
	self.user_count = user_count;
	const obj = self._get_obj();
	if (!obj)
		throw new Error('Invalid object');

	self.ui_id = ScoredObjectUI.next_id;
	ScoredObjectUI.next_id++;
	obj.ui[self.ui_id] = self;

	this.element = new DOMElement('span', {
		title: self._get_title(obj),
		classes: ['word']
	});
	this.element.add_text(self._get_text(obj));
	this._update_colour(obj);
};
ScoredObjectUI.next_id = 0;

ScoredObjectUI.prototype._update_colour = function(obj) {
	this.element.element.style.backgroundColor =
		scoreColour(obj.normalised_score);
};

ScoredObjectUI.prototype._get_title = function(obj) {
	const self = this;
	return (self.user_count + ' occurrances; '
		+ ((obj.count > 0)
			? ('score ' + obj.normalised_score)
			: 'NEW'));
};

ScoredObjectUI.prototype.update = function(obj) {
	const self = this;
	if (obj === undefined)
		obj = self._get_obj();

	self.element.element.title = self._get_title(obj);
	self._update_colour(obj);
};

ScoredObjectUI.prototype.destroy = function() {
	const self = this;

	try {
		const obj = self._get_obj();
		if (obj)
			delete obj.ui[self.ui_id];
	} catch (err) {
		/* Never mind */
	}

	self.element.destroy();
};

const HostnameUI = function(id, user_count) {
	ScoredObjectUI.call(this, id, user_count);
};
HostnameUI.prototype = Object.create(ScoredObjectUI.prototype);
HostnameUI.prototype._get_text = function(obj) {
	return obj.hostname;
};
HostnameUI.prototype._get_obj = function() {
	return hostnames[this.id];
};

const WordUI = function(id, user_count) {
	ScoredObjectUI.call(this, id, user_count);
};
WordUI.prototype = Object.create(ScoredObjectUI.prototype);
WordUI.prototype._get_text = function(obj) {
	return obj.word;
};
WordUI.prototype._get_obj = function() {
	return words[this.id];
};

const WordAdjUI = function(key, user_count) {
	ScoredObjectUI.call(this, key, user_count);
};
WordAdjUI.prototype = Object.create(ScoredObjectUI.prototype);
WordAdjUI.prototype._get_text = function(obj) {
	return obj.proceeding().word + '→' + obj.following().word;
};
WordAdjUI.prototype._get_obj = function() {
	return wordadj[this.id];
};

const google_translate_uri = function(text) {
	return ('https://translate.google.com/#auto|en|'
		+ encodeURIComponent(text));
};

const make_translation_link = function(parent, src) {
	parent.add_text(' ');
	let link = parent.add_new_child('a', {
		href: google_translate_uri(src.element.textContent),
		target: '_blank'
	});
	link.add_new_child('sub').add_text('[Translate]');
	link.update = () => {
		link.element.href =
			google_translate_uri(src.element.textContent);
	};
	return link;
};

/*!
 * UI control for a single user
 */
const UserUI = function(uid) {
	const self = this;

	self.uid = uid;
	self.selected = false;
	const user = users[uid];

	if (user === undefined)
		throw new Error('Unknown user');

	user.ui = self;
	self.auto_classify = true;
	self.first_seen_by_mod = null;

	/* Build the core elements */
	self.element = new DOMElement('div', {
		classes: ['profile'],
		onmouseover: () => {
			if (!busy) {
				self.mark_seen_by_mod();
				self._update_classification();
			}
		},
		onclick: () => {
			self.mark_seen_by_mod();
			if (selected_uid !== self.uid)
				self.select(false);
		}
	});

	self.avatarImg = self.element.add_new_child('div', {
		classes: ['avatar_box']
	}).add_new_child('img', {
		src: '/avatar/' + user.avatar_id
			+ '?width=300&height=300',
		classes: ['avatar']
	});

	let profile_box = self.element.add_new_child('div');
	self.profileLink = profile_box.add_new_child('a', {
		href: user.url,
		target: '_blank'
	});

	self.profileName = self.profileLink.add_new_child('tt');
	self.profileName.element.innerHTML = user.screen_name;

	self.profileLink.add_text(' [#' + uid + ']');

	self.profileTranslateLink = make_translation_link(
		profile_box, self.profileName);

	self.statusField = self.element.add_new_child('div');
	if (user.pending)
		self.statusField.add_text(
			'Re-inspection pending '
			+ user.next_inspection
			+ '; '
			+ user.inspections
			+ ' inspections.');

	let date_list = self.element.add_new_child('div').add_new_child('ul');
	let item = date_list.add_new_child('li');
	item.add_text('Registered: ')
	self.registeredField = item.add_text(user.had_created);

	item = date_list.add_new_child('li');
	item.add_text('First seen: ')
	self.firstSeenField = item.add_text(user.created);

	item = date_list.add_new_child('li');
	item.add_text('Last update: ')
	self.lastUpdateField = item.add_text(user.last_update);

	let score_text_box = self.element.add_new_child('div');
	let score = user.score();

	score_text_box.add_text('Score: ');
	self.scoreField = score_text_box.add_text(score);

	self.scoreGauge = new ScoreGauge(score);
	self.element.add_children(self.scoreGauge.gaugeBox);

	let group_box = self.element.add_new_child('div');
	group_box.add_new_child('div').add_text('Groups: ');
	self.groupField = group_box.add_new_child('ul');
	self._update_groups(user);

	let classify_ctl = self.element.add_new_child('div');
	let classify_frm = classify_ctl.add_new_child('form');

	self.classifySuspectBtn = classify_frm.add_new_child('input', {
		id: 'u' + uid + 'ClassifySuspectBtn',
		type: 'radio',
		value: 'suspect',
		name: 'classification',
		checked: false,
		classes: ['classify_op'],
		onchange: () => {
			self.mark_seen_by_mod();
			if (!self.classifySuspectBtn.element.checked)
				return;

			selected_uid = self.uid;

			self.auto_classify = false;
			self.set_action('suspect');
		}
	});
	self.classifySuspectLbl = classify_frm.add_new_child('label', {
		htmlFor: self.classifySuspectBtn.element.id,
		classes: ['classify_op']
	});
	self.classifySuspectLbl.add_text('Suspect');

	self.classifyNoneBtn = classify_frm.add_new_child('input', {
		id: 'u' + uid + 'ClassifyNeutralBtn',
		type: 'radio',
		value: 'neutral',
		name: 'classification',
		checked: true,
		classes: ['classify_op', 'classify_op_selected'],
		onchange: () => {
			self.mark_seen_by_mod();
			if (!self.classifyNoneBtn.element.checked)
				return;

			selected_uid = self.uid;

			self.auto_classify = false;
			self.set_action(null);
		}
	});
	self.classifyNoneLbl = classify_frm.add_new_child('label', {
		htmlFor: self.classifyNoneBtn.element.id,
		classes: ['classify_op', 'classify_op_selected']
	});
	self.classifyNoneLbl.add_text('Neutral');

	self.classifyLegitBtn = classify_frm.add_new_child('input', {
		id: 'u' + uid + 'ClassifyLegitBtn',
		type: 'radio',
		value: 'legit',
		name: 'classification',
		checked: false,
		classes: ['classify_op'],
		onchange: () => {
			self.mark_seen_by_mod();
			if (!self.classifyLegitBtn.element.checked)
				return;

			selected_uid = self.uid;

			self.auto_classify = false;
			self.set_action('legit');
		}
	});
	self.classifyLegitLbl = classify_frm.add_new_child('label', {
		htmlFor: self.classifyLegitBtn.element.id,
		classes: ['classify_op']
	});
	self.classifyLegitLbl.add_text('Legit');

	classify_frm.add_text(' ');
	classify_frm.add_new_child('button', {
		onclick: () => {
			self.hide();
		}
	}).add_text('Hide');

	let tags_box = self.element.add_new_child('div');
	tags_box.add_new_child('span').add_text('Tags: ');
	self.tagsField = tags_box.add_new_child('span');
	self._update_tags(user);

	let location_box = self.element.add_new_child('div');
	location_box.add_new_child('span').add_text('Location: ');
	self.locationField = location_box.add_new_child('span');
	self.locationField.element.innerHTML = user.location;

	self.locationTranslateLink = make_translation_link(
		location_box, self.locationField);

	let about_me_box = self.element.add_new_child('div');
	about_me_box.add_new_child('span').add_text('About Me: ');
	self.aboutMeField = about_me_box.add_new_child('span');
	self.aboutMeField.element.innerHTML = user.about_me;

	self.aboutMeTranslateLink = make_translation_link(
		about_me_box, self.aboutMeField);

	let who_am_i_box = self.element.add_new_child('div');
	who_am_i_box.add_new_child('span').add_text('Who Am I: ');
	self.whoAmIField = who_am_i_box.add_new_child('span');
	self.whoAmIField.element.innerHTML = user.who_am_i;

	self.whoAmITranslateLink = make_translation_link(
		who_am_i_box, self.whoAmIField);

	let what_i_would_like_to_do_box = self.element.add_new_child('div');
	what_i_would_like_to_do_box.add_new_child('span').add_text(
		'What I Would Like To Do: ');
	self.whatIWouldLikeToDoField =
		what_i_would_like_to_do_box.add_new_child('span');
	self.whatIWouldLikeToDoField.element.innerHTML =
		user.what_i_would_like_to_do;

	self.whatIWouldLikeToDoTranslateLink = make_translation_link(
		what_i_would_like_to_do_box, self.whatIWouldLikeToDoField);

	let project_box = self.element.add_new_child('div');
	project_box.add_text('Projects: ');
	self.projectsField = project_box.add_text(user.projects);

	let links_box = self.element.add_new_child('div');
	links_box.add_new_child('div').add_text('Links:');
	self.linksField = links_box.add_new_child('ul');
	self._update_links(user);

	let tokens_box = self.element.add_new_child('div');
	tokens_box.add_new_child('span').add_text('Tokens:');
	self.tokensField = tokens_box.add_new_child('ul');
	self._update_tokens(user);

	let hostnames_box = self.element.add_new_child('div');
	hostnames_box.add_new_child('div').add_text('Hostnames:');
	self.hostnamesField = hostnames_box.add_new_child('div');
	self._hostnames = [];
	self._update_hostnames(user);

	let words_box = self.element.add_new_child('div');
	words_box.add_new_child('div').add_text('Words:');
	self.wordsField = words_box.add_new_child('div');
	self._words = [];
	self._update_words(user);

	let wordadj_box = self.element.add_new_child('div');
	wordadj_box.add_new_child('div').add_text('Word Adjacencies:');
	self.wordAdjField = wordadj_box.add_new_child('div');
	self._wordadj = [];
	self._update_wordadj(user);
};

UserUI.prototype.mark_seen_by_mod = function() {
	if (this.first_seen_by_mod === null)
		this.first_seen_by_mod = Date.now();
};

UserUI.prototype.seen_by_mod = function() {
	return ((Date.now() - (this.first_seen_by_mod || 0)) > 5000);
};

UserUI.prototype._get_user = function() {
	return users[this.uid];
};

UserUI.prototype.set_action = function(action) {
	const self = this,
		user = self._get_user();
	if (!user)
		return;
	
	if (user.action() === action)
		return;

	user.set_action(action);
	if (action === 'legit') {
		self.classifyLegitBtn.element.checked = true;
		self.classifySuspectBtn.element.checked = false;
		self.classifyNoneBtn.element.checked = false;

		[
			self.classifySuspectBtn,
			self.classifySuspectLbl,
			self.classifyNoneBtn,
			self.classifyNoneLbl
		].forEach((e) => {
			e.rm_classes('classify_op_selected');
		});
		[
			self.classifyLegitBtn,
			self.classifyLegitLbl
		].forEach((e) => {
			e.add_classes('classify_op_selected');
		});
	} else if (action === 'suspect') {
		self.classifySuspectBtn.element.checked = true;
		self.classifyLegitBtn.element.checked = false;
		self.classifyNoneBtn.element.checked = false;


		[
			self.classifyLegitBtn,
			self.classifyLegitLbl,
			self.classifyNoneBtn,
			self.classifyNoneLbl
		].forEach((e) => {
			e.rm_classes('classify_op_selected');
		});
		[
			self.classifySuspectBtn,
			self.classifySuspectLbl
		].forEach((e) => {
			e.add_classes('classify_op_selected');
		});
	} else {
		self.classifyNoneBtn.element.checked = true;
		self.classifyLegitBtn.element.checked = false;
		self.classifySuspectBtn.element.checked = false;


		[
			self.classifySuspectBtn,
			self.classifySuspectLbl,
			self.classifyLegitBtn,
			self.classifyLegitLbl
		].forEach((e) => {
			e.rm_classes('classify_op_selected');
		});
		[
			self.classifyNoneBtn,
			self.classifyNoneLbl
		].forEach((e) => {
			e.add_classes('classify_op_selected');
		});
	}
};

UserUI.prototype._update_classification = function(user) {
	const self = this;
	if (!self.auto_classify)
		return;

	if (!self.seen_by_mod()) {
		/* Set a timeout and see if the moderator has seen it then. */
		setTimeout(self._update_classification.bind(self), 5000);
		return;
	}

	if (user === undefined) {
		user = this._get_user();
		if (!user)
			this.destroy();
	}

	if (!user.pending && user.groups.has(Group.get('auto_legit'))) {
		self.set_action('legit');
	}
};

UserUI.prototype._update_groups = function(user) {
	const self = this;
	self.groupField.clear();
	user.groups.elements().forEach((g) => {
		self.groupField.add_new_child('li').add_text(g.name);
	});
};

UserUI.prototype._update_tags = function(user) {
	const self = this;
	self.tagsField.clear();
	user.tags.forEach((t) => {
		self.tagsField.add_new_child('li').add_text(t);
	});
};

UserUI.prototype._update_links = function(user) {
	const self = this;
	self.linksField.clear();
	user.links.forEach(function (link) {
		let link_tag = self.linksField.add_new_child(
			'li').add_new_child('a', {
				href: link.url
			});
		link_tag.add_text(link.title);
		link_tag.add_new_child('tt').add_text(
			' <' + htmlEscape(link.url) + '>');
	});
};

UserUI.prototype._update_tokens = function(user) {
	const self = this;
	self.tokensField.clear();
	Object.keys(user.tokens).forEach(function (token) {
		let item = self.tokensField.add_new_child('li');
		item.add_new_child('tt').add_text(htmlEscape(token));

		item.add_text(' ' + user.tokens[token] + ' instances');
	});
};

UserUI.prototype._add_to_field = function(field, elements) {
	field.add_children.apply(field,
		elements.map((ui) => {
			return ui.element;
		}).reduce((acc, cur, idx, src) => {
			return acc.concat(cur, document.createTextNode(' '))
		}, []));
};

UserUI.prototype._update_hostnames = function(user) {
	const self = this;

	self._hostnames.forEach((h) => {
		h.destroy();
	});

	self._hostnames = user.hostnames.elements().sort(
		ScoredObject.sortByScore
	).map(function (h) {
		return new HostnameUI(h.id, user.hostname_count[h.id] || 0);
	});

	self._add_to_field(self.hostnamesField, self._hostnames);
};

UserUI.prototype._update_words = function(user) {
	const self = this;

	self._words.forEach((w) => {
		w.destroy();
	});

	self._words = user.words.elements().sort(
		ScoredObject.sortByScore
	).map(function (w) {
		return new WordUI(w.id, user.word_count[w.id] || 0);
	});
	self._add_to_field(self.wordsField, self._words);
};

UserUI.prototype._update_wordadj = function(user) {
	const self = this;

	self._wordadj.forEach((w) => {
		w.destroy();
	});

	self._wordadj = user.wordadj.elements().sort(
		ScoredObject.sortByScore
	).map(function (wa) {
		return new WordAdjUI(wa.key, user.word_count[wa.key] || 0);
	});
	self._add_to_field(self.wordAdjField, self._wordadj);
};

UserUI.prototype.destroy = function() {
	const self = this;
	this.destroy = () => {};

	let ui_idx = user_uis.findIndex((ui) => {
		return (ui.uid === self.uid);
	});
	if (ui_idx >= 0) {
		user_uis.slice(ui_idx, 1);
	}

	const user = self._get_user();
	if (user)
		user.ui = null;

	self._hostnames.forEach((h) => {
		h.destroy();
	});

	self._words.forEach((w) => {
		w.destroy();
	});

	self._wordadj.forEach((wa) => {
		wa.destroy();
	});

	self.element.destroy();
};

UserUI.prototype.hide = function() {
	if (selected_uid === this.uid) {
		/* Select another UID */
		if (prev_selected_uid < this.uid) {
			selectPrev();
		} else {
			selectNext();
		}
	}

	this.auto_classify = false;
	this.set_action(null);
	this.destroy();
}

UserUI.prototype.select = function(scroll) {
	if (scroll === undefined)
		scroll = true;

	user_uis.forEach((ui) => {
		if (ui.uid !== this.uid)
			ui.deselect();
	});

	if (!busy)
		this._update_classification();

	selected_uid = this.uid;
	this.selected = true;
	this.element.add_classes('profile_selected');
	if (scroll)
		this.element.element.scrollIntoView();
};

UserUI.prototype.deselect = function() {
	if (selected_uid === this.uid) {
		selected_uid = null;
		prev_selected_uid = this.uid;
	}
	this.selected = false;
	this.element.rm_classes('profile_selected');
};

UserUI.prototype.refresh = function() {
	const self = this;
	const user = self._get_user();
	if (!user)
		throw new Error('Unknown user');

	self.avatarImg.src = '/avatar/' + user.avatar_id
			+ '?width=300&height=300';
	self.profileLink.href = user.url;
	self.locationField.element.innerHTML = user.location;
	self.locationTranslateLink.update();
	self.aboutMeField.element.innerHTML = user.about_me;
	self.aboutMeTranslateLink.update();
	self.profileName.element.innerHTML = user.screen_name;
	self.profileTranslateLink.update();
	self.whoAmIField.element.innerHTML = user.who_am_i;
	self.whoAmITranslateLink.update();
	self.whatIWouldLikeToDoField.element.innerHTML =
		user.what_i_would_like_to_do;
	self.whatIWouldLikeToDoTranslateLink.update();
	self.projectsField.data = user.projects;

	self.statusField.clear();
	if (user.pending)
		self.statusField.add_text(
			'Re-inspection pending '
			+ user.next_inspection
			+ '; '
			+ user.inspections
			+ ' inspections.');

	const score = user.score();

	self.scoreField.data = score;
	self.scoreGauge.set(score);

	self.registeredField.data = user.had_created;
	self.firstSeenField.data = user.created;
	self.lastUpdateField.data = user.last_update;

	self._update_groups(user);
	self._update_tags(user);
	self._update_links(user);
	self._update_tokens(user);
	self._update_hostnames(user);
	self._update_words(user);
	self._update_wordadj(user);
};

/*!
 * Generate a style colour based on the score.
 */
const scoreColour = function (score) {
	if ((typeof score) !== 'number')
		throw new Error('Unexpected data type: ' + (typeof score));
	var red = Math.round(((score > 0) ? (1.0 - score) : 1.0)*255);
	var grn = Math.round(((score < 0) ? (score + 1.0) : 1.0)*255);
	return 'rgb(' + red + ', ' + grn + ', 0)';
};

/*!
 * Generate a loading spinner
 */
const Spinner = function (message, delay) {
	this.message = message;
	this._dots = '';
	this._spinner = '-';
	this.element = document.createTextNode(this.getSpinnerText());
	this.delay = delay || 250;
	this.timeout = null;
};

Spinner.prototype.getSpinnerText = function() {
	return this.message + (this._dots || '') + this._spinner;
};

Spinner.prototype.nextState = function() {
	switch (this._spinner) {
	case '-':	this._spinner = '\\';	break;
	case '\\':	this._spinner = '|'; break;
	case '|':	this._spinner = '/'; break;
	default:
			this._spinner = '-';
			this._dots += '.';
			break;
	}
};

Spinner.prototype.update = function() {
	this.element.data = this.getSpinnerText();
	this.nextState();
};

Spinner.prototype.start = function() {
	const self = this;
	if (self.timeout !== null)
		throw new Error('Already running');
	self.timeout = setTimeout(self._go.bind(self), self.delay);
};

Spinner.prototype.stop = function() {
	const self = this;
	if (self.timeout !== null) {
		clearTimeout(self.timeout);
		self.timeout = null;
	}
};

Spinner.prototype._go = function() {
	const self = this;
	self.update();
	self.timeout = setTimeout(self._go.bind(self), self.delay);
}

const getNextPage = function(subset) {
	var uri = source;

	let spinner = new Spinner('Loading user accounts');
	status_pane.clear();
	status_pane.add_children(spinner.element);

	if ((subset === 'newer') && (newest_uid !== null)) {
		spinner.message += ' after UID #' + newest_uid;
		uri += "?after_user_id=" + newest_uid
			+ '&order=asc';
	} else if ((subset === 'older') && (oldest_uid !== null)) {
		spinner.message += ' before UID #' + oldest_uid;
		uri += "?before_user_id=" + oldest_uid
			+ '&order=desc';
	} else {
		let args = [];

		if (newest_uid !== null)
			args.push('before_user_id=' + (newest_uid+1));

		if (oldest_uid !== null)
			args.push('after_user_id=' + (oldest_uid-1));

		if (args.length)
			uri += '?' + args.join('&');

		/* Reset so we can update with what actually got returned */
		newest_uid = null;
		oldest_uid = null;
	}
	spinner.start();
	busy = true;

	return get_json(uri).then(function (data) {
		spinner.stop();
		status_pane.clear();
		busy = false;

		heading.clear();
		heading.add_text('Hackaday.io Spam Hunter Project');

		switch(source) {
		case '/data/newcomers.json':
			heading.add_text(': Newest unclassified users');
			break;
		case '/data/legit.json':
			heading.add_text(': Newest legitmate users');
			break;
		case '/data/suspect.json':
			heading.add_text(': Newest suspect users');
			break;
		}

		if (subset === 'newer')
			data.users.reverse();

		let widgets = data.users.map(function (user) {
			let u = User.from_data(user);

			if ((newest_uid === null)
				|| (newest_uid < u.id))
				newest_uid = u.id;

			if ((oldest_uid === null)
				|| (oldest_uid > u.id))
				oldest_uid = u.id;

			let uui = new UserUI(u.id);
			user_uis.push(uui);
			return uui.element;
		});

		user_uis.sort((a, b) => {
			if (a.uid < b.uid)
				return -1;
			else if (a.uid > b.uid)
				return 1;
			return 0;
		});

		widgets.unshift((subset === 'newer') ? 'start' : 'end');
		user_pane.add_children.apply(user_pane, widgets);
		user_pane.element.focus();

		if ((selected_uid === null) && (user_uis.length)) {
			user_uis[(subset === 'newer') ?
				0 : (user_uis.length-1)].select();
		}

		update_pending_actions();

		if (user_pane.element.childNodes.length === 0) {
			const no_users_box = user_pane.add_new_child('div');

			no_users_box.add_new_child('h1').add_text(
				'No users found');

			no_users_box.add_new_child('br');

			no_users_box.add_new_child('button', {
				onclick: function() {
					no_users_box.destroy();
					getNextPage('newer');
				}
			}).add_text('Load newer users');

			no_users_box.add_new_child('br');

			no_users_box.add_new_child('button', {
				onclick: function() {
					no_users_box.destroy();
					getNextPage('older');
				}
			}).add_text('Load older users');
		}

		/* Update the hash with the new start/end UID */
		let location_args = [
			'source=' + encodeURIComponent(source)
		];
		if (oldest_uid !== null) {
			location_args.push(
				'oldest_uid='
				+ encodeURIComponent(oldest_uid));
			heading.firstUIDField.element.value = oldest_uid;
		} else {
			heading.firstUIDField.element.value = '';
		}

		if (newest_uid !== null) {
			location_args.push(
				'newest_uid='
				+ encodeURIComponent(newest_uid));
			heading.lastUIDField.element.value = newest_uid;
		} else {
			heading.lastUIDField.element.value = '';
		}
		location.hash = '#' + location_args.join('&');

	}).catch(function (err) {
		spinner.stop();
		status_pane.clear();
		status_pane.add_text('Failed to fetch users: ' + err.message);
		busy = false;
		console.log(err);

		user_pane.element.focus();
		setTimeout(update_pending_actions, 10000);
	});
};

/*! Update the listings of pending actions. */
const update_pending_actions = function() {
	if (!busy) {
		let legit = 0, suspect = 0;
		Object.keys(user_actions).forEach((uid) => {
			const action = user_actions[uid];
			if (action === 'legit')
				legit++;
			else if (action === 'suspect')
				suspect++;
		});

		status_pane.clear();

		if (legit || suspect) {
			let listing = [];
			if (legit)
				listing.push(legit + ' legit users');
			if (suspect)
				listing.push(suspect + ' suspect users');

			status_pane.add_text('Pending operations: '
				+ listing.join(', ') + '.');
			status_pane.add_new_child('button', {
				onclick: () => {
					commitPending();
				}
			}).add_text('Commit');
		}
	}
};

const getUserUI = function(uid) {
	if (uid === undefined)
		uid = selected_uid;
	if (uid === null)
		return;

	return user_uis.find((ui) => {
		return (ui.uid === uid);
	}) || null;
};

const findPrevUser = function() {
	if (selected_uid !== null) {
		let candidates = user_uis.filter((ui) => {
			return (ui.uid > selected_uid);
		});

		if (candidates.length) {
			return candidates[0];
		}
	} else if (user_uis.length) {
		return user_uis[0];
	} else {
		return null;
	}
};

const findNextUser = function() {
	if (selected_uid !== null) {
		let candidates = user_uis.filter((ui) => {
			return (ui.uid < selected_uid);
		});

		if (candidates.length) {
			return candidates[candidates.length-1];
		}
	} else if (user_uis.length) {
		return user_uis[user_uis.length-1];
	} else {
		return null;
	}
};

const selectPrev = function() {
	let ui = findPrevUser();
	if (ui) {
		ui.select();
	} else {
		getNextPage('newer').then(() => {
			ui = findPrevUser();
			if (ui)
				ui.select();
		});
	}
};

const selectNext = function() {
	let ui = findNextUser();
	if (ui) {
		ui.select();
	} else {
		getNextPage('older').then(() => {
			ui = findNextUser();
			if (ui)
				ui.select();
		});
	}
};

const commitPending = function() {
	const total = Object.keys(user_actions).length;
	let failures = 0;
	let spinner = new Spinner('Committing actions (' + total + ' to do)');

	status_pane.clear();
	status_pane.add_children(spinner.element);
	spinner.start();
	busy = true;

	const update_spinner_msg = function() {
		const remain = Object.keys(user_actions).length - failures,
			done = total - remain;

		let comments = [];
		if (done)
			comments.push(done + ' of ' + total + ' done');
		if (failures)
			comments.push(failures + ' failed');

		spinner.message = 'Committing actions';
		if (comments.length)
			spinner.message += ' (' + comments.join(', ') + ')';
		else
			spinner.message += ' (' + total + ' to do)';
	};

	update_spinner_msg();

	let promises = Object.keys(user_actions).map((uid) => {
		const action = user_actions[uid],
			user = users[uid],
			user_ui = user_uis.find((ui) => {
				return (ui.uid == uid);
			});

		if (user !== undefined) {
			return user.commit().then(() => {
				update_spinner_msg();
				return new Promise((resolve, reject) => {
					setTimeout(resolve, 3000);
				});
			}).then(() => {
				if (user_ui)
					user_ui.hide();
			}).catch((err) => {
				console.log('Failed to commit #'
					+ uid
					+ ': '
					+ err.message
					+ '\n'
					+ err.stack);
				failures++;
				update_spinner_msg();
			});
		} else {
			return Promise.resolve();
		}
	});

	Promise.all(promises).then(() => {
		busy = false;
		spinner.stop();
		update_pending_actions();
		if (user_uis.length === 0)
			return getNextPage('older');
	}).catch((err) => {
		busy = false;
		spinner.stop();
		update_pending_actions();
		console.log('Failed to commit: '
			+ err.message
			+ '\n'
			+ err.stack);
	});
};

const main = function() {
	clear_element(document.body);

	const parse_args = function(args) {
		args.split('&').forEach((arg) => {
			try {
				const parts = arg.split('=', 2);
				const name = parts[0];
				const value = decodeURIComponent(parts[1]);

				switch(name) {
				case 'newest_uid':
					newest_uid = parseInt(value);
					break;
				case 'oldest_uid':
					oldest_uid = parseInt(value);
					break;
				case 'source':
					source = value;
					break;
				}
			} catch (err) {
				console.log('Failed to decode ' + arg
					+ ': ' + err.message + '\n'
					+ err.stack);
			}
		});
	}

	let query_and_hash = window.location.href.substring(
		window.location.origin.length
		+ window.location.pathname.length);

	if (query_and_hash.substring(0,1) === '?') {
		/* Parse the query string first */
		let query = query_and_hash.substring(1);
		if (window.location.hash.length) {
			query = query.substring(0,
					query.length
				- window.location.hash.length);
		} else if (query.substring(query.length-1) === '#') {
			query = query.substring(0, query.length-1);
		}
		parse_args(query);
	}

	if (window.location.hash) {
		parse_args(window.location.hash.substring(1));
	}

	title_pane = new DOMElement('div', {
		tabIndex: 2,
		classes: ['title_pane']
	});
	status_pane = new DOMElement('div', {
		tabIndex: 1,
		classes: ['status_pane']
	});
	user_pane = new DOMElement('div', {
		classes: ['user_pane'],
		tabIndex: 0,
		onscroll: function(ev) {
			if (busy)
				return;

			if (this.scrollTop === this.scrollTopMax) {
				getNextPage('older');
			} else if (this.scrollTop === 0) {
				getNextPage('newer');
			}
		}
	});

	heading = title_pane.add_new_child('h1');
	heading.add_text('Hackaday.io Spam Hunter Project');

	let head_form = title_pane.add_new_child('form');
	head_form.add_text('Display user IDs: ');
	heading.firstUIDField = head_form.add_new_child('input', {
		type: 'text',
		name: 'oldest_uid',
		value: oldest_uid,
		size: 2
	});
	head_form.add_text(' to ');
	heading.lastUIDField = head_form.add_new_child('input', {
		type: 'text',
		name: 'newest_uid',
		value: newest_uid,
		size: 2
	});
	head_form.add_text(' from set ');

	heading.srcNewestBtn = head_form.add_new_child('input', {
		type: 'radio',
		name: 'source',
		id: 'srcNewestBtn',
		value: '/data/newcomers.json',
		checked: (source === '/data/newcomers.json')
	});
	head_form.add_new_child('label', {
		htmlFor: 'srcNewestBtn'
	}).add_text('Newest Users');

	heading.srcLegitBtn = head_form.add_new_child('input', {
		type: 'radio',
		name: 'source',
		id: 'srcLegitBtn',
		value: '/data/legit.json',
		checked: (source === '/data/legit.json')
	});
	head_form.add_new_child('label', {
		htmlFor: 'srcLegitBtn'
	}).add_text('Legit Users');

	heading.srcSuspectBtn = head_form.add_new_child('input', {
		type: 'radio',
		name: 'source',
		id: 'srcSuspectBtn',
		value: '/data/suspect.json',
		checked: (source === '/data/suspect.json')
	});
	head_form.add_new_child('label', {
		htmlFor: 'srcSuspectBtn'
	}).add_text('Suspect Users');

	heading.srcAdminBtn = head_form.add_new_child('input', {
		type: 'radio',
		name: 'source',
		id: 'srcAdminBtn',
		value: '/data/admin.json',
		checked: (source === '/data/admin.json')
	});
	head_form.add_new_child('label', {
		htmlFor: 'srcAdminBtn'
	}).add_text('Admin Users');

	head_form.add_new_child('input', {
		type: 'submit',
		value: 'Fetch',
		onclick: () => {
			location.hash = '';
		}
	});

	document.body.appendChild(title_pane.element);
	document.body.appendChild(user_pane.element);
	document.body.appendChild(status_pane.element);

	user_pane.element.focus();
	user_pane.element.addEventListener('keypress', (ev) => {
		/* Ignore CTRL/ALT keys */
		if (ev.ctrlKey || ev.altKey)
			return;

		if (ev.key === 'b') {
			selectPrev();
			ev.preventDefault();
		} else if (ev.key === 'n') {
			selectNext();
			ev.preventDefault();
		} else if (ev.key === 'h') {
			let ui = getUserUI();
			if (ui) {
				ui.hide();
			}
			ev.preventDefault();
		} else if (ev.key === 'u') {
			let ui = getUserUI();
			if (ui) {
				ui.set_action(null);
			}
			ev.preventDefault();
		} else if (ev.key === 's') {
			let ui = getUserUI();
			if (ui) {
				ui.set_action('suspect');
			}
			ev.preventDefault();
		} else if (ev.key === 'l') {
			let ui = getUserUI();
			if (ui) {
				ui.set_action('legit');
			}
			ev.preventDefault();
		} else if (ev.key === 'c') {
			commitPending();
			ev.preventDefault();
		}
	});

	getNextPage('init');
};
